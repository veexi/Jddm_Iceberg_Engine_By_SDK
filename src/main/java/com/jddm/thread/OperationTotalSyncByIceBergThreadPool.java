package com.jddm.thread;


import com.dsg.analysis.vo.PackageReturnVo;
import com.dsg.analysis.vo.Udb_BcolumnVo;
import com.jddm.common.Constant;
import com.jddm.conf.GlobalConfInfo;
import com.jddm.conf.GlobalSetConfInfo;
import com.jddm.vo.RowOperation;
import com.jddm.vo.SequencedPackage;
import com.jddm.operation.IceBergBatchOperationHandler;
import com.publics.common.ConstantPubSet;
import com.publics.common.ConstantPublic;

import org.apache.iceberg.data.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * IceBerg engine worker: processes CDC packets, builds RowOperation list,
 * appends to IceBergSchemaImmuTableOpsMap. cflag & 1 == 1 -> before image, else -> after.
 * Trajectory mode: all I/U/D -> insert. Transaction mode: I->insert, U->update, D->delete.
 */
public class OperationTotalSyncByIceBergThreadPool extends Thread {

    public Logger log = LogManager.getLogger(OperationTotalSyncByIceBergThreadPool.class);

    private Lock threadlock = new ReentrantLock();
    // 原来的 TIMESTAMP_PATTERNS 字符串数组删掉，换成预编译好的 Formatter 数组
// DateTimeFormatter 是线程安全的，直接 static final 共享
    private static final java.time.format.DateTimeFormatter[] TIMESTAMP_FORMATTERS = {
            java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS"),
            java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"),
            java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
            java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"),
            java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd"),
            java.time.format.DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss"),
            java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"),
            java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss.SSS"),
            java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS"),

    };
    private static final java.time.format.DateTimeFormatter DATE_FMT_SLASH =
            java.time.format.DateTimeFormatter.ofPattern("yyyy/MM/dd");
    private static final java.time.format.DateTimeFormatter TIME_FMT_MICROSECOND =
            java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS");
    private static final java.time.format.DateTimeFormatter TIME_FMT_MILLISECOND =
            java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    // 对应标记哪些是纯日期格式（没有时间部分，匹配后要补 00:00:00）
    private static final boolean[] TIMESTAMP_IS_DATE_ONLY = {
            false, false, false, false, false, true, false, true
    };

    // withZone 分支同理，预编译带时区的 Formatter
    private static final java.time.format.DateTimeFormatter[] ZONED_FORMATTERS = {
            java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS XXX"),
            java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS XXX"),
            java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS XXX"),
            java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss XXX"),
    };
    @Override
    public void run() {
        SequencedPackage seqPackage;
        PackageReturnVo packageReturnVo;

        long startTimer = 0L;
        String schemaKeyByParquet = "";
        String schemaKeyByParquetThreadID = "";
        int rowsNum = 0;
        Map<String, Udb_BcolumnVo> rowUdbColumnMap = new HashMap<>();
        int columnsNum = 0;
        long threadID = Thread.currentThread().getId();
        String colNameByNumberKey = "";

        GlobalConfInfo.txtFileNoSpliMap.put(threadID, new AtomicInteger(0));

        while (true) {
            try {
                if (!ConstantPublic.jddmEngineStatFlag) {
                    continue;
                }
                while (GlobalSetConfInfo.icebergEngineOperationQueue.size() > 10000) {
                    log.warn("[IceBergPool][tid={}] queue backpressure, waiting... size={}",
                            Thread.currentThread().getId(),
                            GlobalSetConfInfo.icebergEngineOperationQueue.size());
                    Thread.sleep(500);
                }
                Object recvPackageObj = GlobalSetConfInfo.icebergEngineOperationQueue.take();
                if (!(recvPackageObj instanceof SequencedPackage)) {
                    continue;
                }

                seqPackage              = (SequencedPackage) recvPackageObj;
                packageReturnVo         = seqPackage.getPackageVo();
                long pktSeq             = seqPackage.getSequence();
                rowUdbColumnMap         = packageReturnVo.getRowUdbColumnMap();
                rowsNum                 = Integer.parseInt(packageReturnVo.getRowsCount());
                columnsNum              = Integer.parseInt(packageReturnVo.getColsCount());
                schemaKeyByParquet      = packageReturnVo.getOwnerName().toLowerCase()
                        + "." + packageReturnVo.getTableName().toLowerCase();
                schemaKeyByParquetThreadID = schemaKeyByParquet + "." + threadID;
                colNameByNumberKey = "";

                if (GlobalSetConfInfo.IceBergSchemaCahceMap.get(schemaKeyByParquet) == null
                        || !GlobalSetConfInfo.IceBergCacheTableMap.containsKey(schemaKeyByParquet)) {
                    log.warn("[IceBergPool][tid={}] table {} schema not loaded, skip packet",
                            Thread.currentThread().getId(), schemaKeyByParquet);
                    continue;
                }

                initCacheIfAbsent(schemaKeyByParquet, schemaKeyByParquetThreadID);

                // ★ 改动：保留原始 rawOpType，全量 "i" 在 toUpperCase 前判断，否则转完就无法区分
                String rawOpType = packageReturnVo.getOperationType();
                boolean isFullLoad = "i".equals(rawOpType); // 小写 i = 全量
                String opType = rawOpType.toUpperCase();

                List<RowOperation> opsBuffer = buildRowOperations(
                        opType, isFullLoad, rowsNum, columnsNum, rowUdbColumnMap,
                        schemaKeyByParquet, schemaKeyByParquetThreadID, colNameByNumberKey,
                        pktSeq);
                rowUdbColumnMap = null;
                packageReturnVo = null;
                seqPackage = null;
                log.info("[IceBergPool][tid={}] pktSeq={} table={} op={} fullLoad={} parsedOps={}", Thread.currentThread().getId(), pktSeq, schemaKeyByParquet, opType, isFullLoad, opsBuffer.size());
                if (Constant.debugLogEnabled) {
                    log.info("[IceBergPool][tid={}] pktSeq={} opDetails={}", Thread.currentThread().getId(), pktSeq, summarizeOps(opsBuffer, schemaKeyByParquet, 10));
                }

                java.util.concurrent.LinkedBlockingDeque<RowOperation> queue = GlobalSetConfInfo.IceBergSchemaImmuTableOpsMap.get(schemaKeyByParquetThreadID);
                synchronized (queue) {
                    queue.addAll(opsBuffer);
                }

                GlobalConfInfo.lastDataWriteTimerByParquetMap
                        .put(schemaKeyByParquetThreadID, System.currentTimeMillis());

                int currentCount = GlobalConfInfo.engineAtomicByTableKeyMap
                        .computeIfAbsent(schemaKeyByParquetThreadID, k -> new AtomicInteger(0))
                        .addAndGet(rowsNum);

                if (currentCount >= Constant.writeCountNoToHiveFile) {
                    log.info("[IcebergPool][tid={}] count rows ::  {}>={}", Thread.currentThread().getId(), currentCount, Constant.writeCountNoToHiveFile);
                    triggerFlush(schemaKeyByParquet, schemaKeyByParquetThreadID, packageReturnVo, currentCount);
                } else {
                    log.info("[IcebergPool][tid={}] count rows ::  {}<{}", Thread.currentThread().getId(), currentCount, Constant.writeCountNoToHiveFile);
                }

            } catch (Exception ex) {
                ex.printStackTrace();
                log.error("[IceBergPool][tid={}] thread exit on error. threadId={}", Thread.currentThread().getId(), threadID, ex);
                break;
            }
        }
    }

    /**
     * Build RowOperation list from packet. baseOffset = pktSeq * 1M for CDC ordering.
     * ★ 改动：新增 isFullLoad 参数，传递到 buildOperation，用于标记全量 insert
     */
    private List<RowOperation> buildRowOperations(
            String opType,
            boolean isFullLoad,
            int rowsNum,
            int columnsNum,
            Map<String, Udb_BcolumnVo> rowUdbColumnMap,
            String schemaKeyByParquet,
            String schemaKeyByParquetThreadID,
            String colNameByNumberKey,
            long pktSeq) {

        List<RowOperation> result = new ArrayList<>(rowsNum);
        long baseOffset = pktSeq * 1_000_000L;
        StringBuilder mapKeyBuilder = new StringBuilder(16);

        StringBuilder debugColLogs = null;
        if (Constant.debugLogEnabled) {
            debugColLogs = new StringBuilder(1024);
        }

        for (int rowNo = 0; rowNo < rowsNum; rowNo++) {

            GenericRecord newRecord = GenericRecord.create(
                    GlobalSetConfInfo.IceBergSchemaCahceMap.get(schemaKeyByParquet));
            GenericRecord oldRecord = isFullLoad ? null :
                    GenericRecord.create(GlobalSetConfInfo.IceBergSchemaCahceMap.get(schemaKeyByParquet));

            boolean hasNewData = false;
            boolean hasOldData = false;
            int mergerColLimit = columnsNum;

            if (Constant.debugLogEnabled) {
                debugColLogs.setLength(0);
            }

            for (int colNo = 0; colNo < columnsNum; colNo++) {
                mapKeyBuilder.setLength(0);
                mapKeyBuilder.append(rowNo).append('-').append(colNo);
                Udb_BcolumnVo columnInfo = rowUdbColumnMap.get(mapKeyBuilder.toString());
                // colNameByNumberKey 保留赋值，兼容外部可能存在的引用
                String colNameLower = columnInfo.getColumnName().toLowerCase();
                if (columnInfo.getColumnName().equals(ConstantPubSet.MergerColKeyName)) {

                    switch (columnInfo.getColumnValue().toUpperCase()) {
                        case "I":
                            opType = "I";
                            mergerColLimit = columnsNum / 2;
                            break;
                        case "U":
                        case "UA":
                        case "UB":
                            opType = "U";
                            break;
                        case "D":
                            opType = "D";
                            mergerColLimit = columnsNum / 2;
                            break;
                    }
                    continue;
                }
                if (colNo > mergerColLimit) {
                    continue;
                }

                if (Constant.debugLogEnabled) {
                    if (debugColLogs.length() > 0) {
                        debugColLogs.append("\n");
                    }
                    debugColLogs.append("[IceBergPool][tid=").append(Thread.currentThread().getId())
                            .append("] table=").append(schemaKeyByParquet)
                            .append(" op=").append(opType)
                            .append(" row=").append(rowNo);
                }

                // =========================================================
                // 全量加载路径：CDC 层数据均为 String，需按 schema 字段类型转换。
                // =========================================================
                if (isFullLoad) {
                    String rawVal = columnInfo.getColumnValue();
                    if (rawVal != null && !rawVal.isEmpty()) {
                        // 直接 O(1) 查缓存，不再每列遍历 schema
                        org.apache.iceberg.types.Type fieldType =
                                GlobalSetConfInfo.columnTypeCache.get(schemaKeyByParquet + "." + colNameLower);
                        Object convertedVal = (fieldType != null)
                                ? convertValue(rawVal, fieldType, colNameLower)
                                : rawVal;
                        if (convertedVal != null) {
                            newRecord.setField(colNameLower, convertedVal);
                        }
                    }
                    hasNewData = true;
                    continue;
                }

                // =========================================================
                // 增量 CDC 路径：通过 cflag 区分前镜像 / 后镜像。
                // =========================================================
                int cflag = columnInfo.getCflag();
                if ((cflag & 1) == 1) {
                    // before image
                    fillRecord(oldRecord, columnInfo, schemaKeyByParquet, opType);
                    hasOldData = true;
                } else {
                    // after image
                    fillRecord(newRecord, columnInfo, schemaKeyByParquet, opType);
                    hasNewData = true;
                }
            }

            if (Constant.debugLogEnabled && debugColLogs.length() > 0) {
                log.info("{}", debugColLogs.toString());
            }

            long offset = baseOffset + rowNo;
            // ★ 改动：传递 isFullLoad 到 buildOperation
            List<RowOperation> ops = buildOperation(opType, isFullLoad, newRecord, oldRecord,
                    hasNewData, hasOldData, offset, schemaKeyByParquet);
            result.addAll(ops);
        }

        return result;

    }

    /**
     * ★ 改动：新增 isFullLoad 参数，全量 INSERT 时在 RowOperation 上打标记
     */
    private List<RowOperation> buildOperation(String opType,
                                              boolean isFullLoad,
                                              GenericRecord newRecord,
                                              GenericRecord oldRecord,
                                              boolean hasNewData,
                                              boolean hasOldData,
                                              long offset,
                                              String tableKeyName) {
        boolean isTransaction = "transaction".equals(Constant.icebergWriteMode);
        boolean hasPk = hasPk(tableKeyName);

        switch (opType) {
            case "U":
                if (isTransaction && hasPk) {
                    if (hasOldData && hasNewData) {
                        List<String> pkNames = GlobalSetConfInfo.TablePkColCacheMap.get(tableKeyName);
                        String oldPkString = extractRecordKey(oldRecord, pkNames);
                        String newPkString = extractRecordKey(newRecord, pkNames);

                        if (!oldPkString.equals(newPkString)) {
                            if (Constant.debugLogEnabled) {
                                log.info("[IceBergPool][tid={}] UPDATE changed PK table={}: {} -> {}, splitting into DELETE + INSERT", Thread.currentThread().getId(), tableKeyName, oldPkString, newPkString);
                            }
                            List<RowOperation> splitOps = new ArrayList<>(2);
                            splitOps.add(RowOperation.delete(oldRecord, offset));
                            splitOps.add(RowOperation.insert(newRecord, offset + 1));
                            return splitOps;
                        }

                        return Collections.singletonList(RowOperation.update(oldRecord, newRecord, offset));
                    }
                    if (!hasOldData && hasNewData) {
                        log.warn("[IceBergPool][tid={}] UPDATE only after-image, fallback delete+insert table={} offset={}", Thread.currentThread().getId(), tableKeyName, offset);
                        List<RowOperation> ops = new ArrayList<>(2);
                        ops.add(RowOperation.delete(newRecord, offset));
                        ops.add(RowOperation.insert(newRecord, offset + 1));
                        return ops;
                    }
                }
                if (hasOldData && !hasNewData) {
                    log.warn("[IceBergPool][tid={}] UPDATE only before-image, delete old row table={} offset={}",
                            Thread.currentThread().getId(), tableKeyName, offset);
                    return Collections.singletonList(RowOperation.delete(oldRecord, offset));
                }
                if (Constant.debugLogEnabled) {
                    log.info("[IceBergPool][tid={}] UPDATE degrade to trajectory hasPk={} hasOld={} hasNew={} table={}", Thread.currentThread().getId(), hasPk, hasOldData, hasNewData, tableKeyName);
                }
                List<RowOperation> updateOps = new ArrayList<>();
                if (hasOldData) {
                    updateOps.add(RowOperation.insert(oldRecord, offset));
                }
                if (hasNewData) {
                    updateOps.add(RowOperation.insert(newRecord, offset + 1));
                }
                return updateOps;

            case "I":
                // ★ 改动：全量 insert 时在 RowOperation 上打 fullLoad 标记，供 flushBatch 走快速路径
                RowOperation insertOp = RowOperation.insert(newRecord, offset);
                if (isFullLoad) {
                    insertOp.setFullLoad(true);
                }
                return Collections.singletonList(insertOp);

            case "D":
                if (!hasOldData && !hasNewData) {
                    log.error("[IceBergPool][tid={}] DELETE skip: no data on either side, table={} offset={}", Thread.currentThread().getId(), tableKeyName, offset);
                    return Collections.emptyList();
                }

                if (isTransaction && hasPk) {
                    GenericRecord deleteRecord = chooseDeleteRecordByPk(
                            tableKeyName, oldRecord, newRecord, hasOldData, hasNewData);
                    if (deleteRecord == null) {
                        log.error("[IceBergPool][tid={}] DELETE skip: PK fill failed, cannot locate row table={} offset={}", Thread.currentThread().getId(), tableKeyName, offset);
                        return Collections.emptyList();
                    }
                    return Collections.singletonList(RowOperation.delete(deleteRecord, offset));
                }

                if (!hasPk && Constant.debugLogEnabled) {
                    log.info("[IceBergPool][tid={}] DELETE degrade to trajectory (no PK) table={}", Thread.currentThread().getId(), tableKeyName);
                }
                List<String> pkNamesForTraj = GlobalSetConfInfo.TablePkColCacheMap.get(tableKeyName);
                GenericRecord fallbackRecord;
                if (hasPk) {
                    if (hasOldData && isPkReady(oldRecord, pkNamesForTraj)) {
                        fallbackRecord = oldRecord;
                    } else if (hasNewData && isPkReady(newRecord, pkNamesForTraj)) {
                        fallbackRecord = newRecord;
                    } else {
                        fallbackRecord = hasOldData ? oldRecord : newRecord;
                    }
                } else {
                    fallbackRecord = hasOldData ? oldRecord : newRecord;
                }
                return Collections.singletonList(RowOperation.insert(fallbackRecord, offset));

            default:
                return Collections.emptyList();
        }
    }

    private GenericRecord chooseDeleteRecordByPk(String tableKeyName,
                                                 GenericRecord oldRecord,
                                                 GenericRecord newRecord,
                                                 boolean hasOldData,
                                                 boolean hasNewData) {
        List<String> pkNames = GlobalSetConfInfo.TablePkColCacheMap.get(tableKeyName);
        if (pkNames == null || pkNames.isEmpty()) {
            return hasOldData ? oldRecord : newRecord;
        }

        boolean oldPkReady = hasOldData && isPkReady(oldRecord, pkNames);
        if (oldPkReady) {
            if (Constant.debugLogEnabled) {
                log.info("[IceBergPool][tid={}] delete choose OLD by PK table={} pkNames={}", Thread.currentThread().getId(), tableKeyName, pkNames);
            }
            return oldRecord;
        }

        boolean newPkReady = hasNewData && isPkReady(newRecord, pkNames);
        if (newPkReady) {
            if (Constant.debugLogEnabled) {
                log.info("[IceBergPool][tid={}] delete choose NEW by PK table={} pkNames={}", Thread.currentThread().getId(), tableKeyName, pkNames);
            }
            return newRecord;
        }

        log.error("[IceBergPool][tid={}] DELETE aborted: PK fill failed on both sides, table={} pkNames={} hasOld={} hasNew={} — check fillRecord type-conversion errors above",
                Thread.currentThread().getId(), tableKeyName, pkNames, hasOldData, hasNewData);
        return null;
    }

    private boolean isPkReady(GenericRecord record, List<String> pkNames) {
        if (record == null || pkNames == null || pkNames.isEmpty()) {
            return false;
        }
        for (String pk : pkNames) {
            Object val = record.getField(pk);
            if (val == null || String.valueOf(val).isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private boolean hasPk(String tableKeyName) {
        List<String> pkNames = GlobalSetConfInfo.TablePkColCacheMap.get(tableKeyName);
        return pkNames != null && !pkNames.isEmpty();
    }

    private String summarizeOps(List<RowOperation> ops, String tableKeyName, int limit) {
        if (ops == null || ops.isEmpty()) {
            return "[]";
        }
        List<String> pkNames = GlobalSetConfInfo.TablePkColCacheMap.get(tableKeyName);
        List<String> out = new ArrayList<>();
        int max = Math.min(ops.size(), limit);
        for (int i = 0; i < max; i++) {
            RowOperation op = ops.get(i);
            GenericRecord record;
            if (op.getType() == RowOperation.OpType.DELETE) {
                record = op.getOldRecord() != null ? op.getOldRecord() : op.getNewRecord();
            } else {
                record = op.getNewRecord() != null ? op.getNewRecord() : op.getOldRecord();
            }
            out.add(op.getType() + (op.isFullLoad() ? "[full]" : "") + "@" + op.getBinlogOffset() + "{"
                    + extractRecordKey(record, pkNames) + "}");
        }
        if (ops.size() > limit) {
            out.add("...+" + (ops.size() - limit));
        }
        return out.toString();
    }

    private String extractRecordKey(GenericRecord record, List<String> pkNames) {
        if (record == null) {
            return "__NULL_RECORD__";
        }
        if (pkNames != null && !pkNames.isEmpty()) {
            return pkNames.stream()
                    .map(pk -> pk + "=" + safeString(record.getField(pk)))
                    .collect(java.util.stream.Collectors.joining(","));
        }
        return record.struct().fields().stream()
                .map(f -> f.name() + "=" + safeString(record.getField(f.name())))
                .collect(java.util.stream.Collectors.joining(","));
    }

    private String safeString(Object v) {
        return v == null ? "__NULL__" : v.toString();
    }

    /**
     * 将 CDC 过来的 String 值按照 Iceberg schema 中的字段类型做转换，再写入 GenericRecord。
     * CDC 层所有字段值均为 String，本方法负责 String → Java 类型（Long/BigDecimal/LocalDateTime/ByteBuffer 等）。
     *
     * @param schemaKeyByParquet 格式为 "owner.tableName"，用于从 IceBergSchemaCahceMap 取 schema
     */
    private void fillRecord(GenericRecord record,
                            Udb_BcolumnVo columnInfo,
                            String schemaKeyByParquet,
                            String opType) {
        try {
            String rawValue = columnInfo.getColumnValue();
            if (rawValue == null || rawValue.isEmpty()) return;

            String colName = columnInfo.getColumnName().toLowerCase();

            // 直接从预建缓存中 O(1) 拿类型，不再每次遍历 Schema
            org.apache.iceberg.types.Type fieldType =
                    GlobalSetConfInfo.columnTypeCache.get(schemaKeyByParquet + "." + colName);
            if (fieldType == null) {
                if (Constant.debugLogEnabled) {
                    log.warn("[IceBergPool][tid={}] col={} not in columnTypeCache schema={}, skip",
                            Thread.currentThread().getId(), colName, schemaKeyByParquet);
                }
                return;
            }

            Object converted = convertValue(rawValue, fieldType, colName);
            if (converted != null) {
                record.setField(colName, converted);
            }
        } catch (Exception e) {
            log.warn("[IceBergPool][tid={}] fillRecord failed col={} val={} err={}",
                    Thread.currentThread().getId(),
                    columnInfo.getColumnName(), columnInfo.getColumnValue(), e.getMessage());
        }
    }

    private void initCacheIfAbsent(String schemaKeyByParquet,
                                   String schemaKeyByParquetThreadID) throws Exception {
        if (GlobalSetConfInfo.IceBergTableGnericCacheMap.containsKey(schemaKeyByParquetThreadID)) {
            return;
        }
        long startTimer = System.currentTimeMillis();
        GenericRecord record = GenericRecord.create(GlobalSetConfInfo.IceBergSchemaCahceMap.get(schemaKeyByParquet));
        GlobalSetConfInfo.IceBergTableGnericCacheMap.put(schemaKeyByParquetThreadID, record);
        GlobalSetConfInfo.IceBergSchemaImmuTableOpsMap
                .putIfAbsent(schemaKeyByParquetThreadID, new LinkedBlockingDeque<>());
        log.info("[IceBergPool][tid={}] cache init done key={} cost={}ms", Thread.currentThread().getId(), schemaKeyByParquetThreadID, System.currentTimeMillis() - startTimer);
    }

    private void triggerFlush(String schemaKeyByParquet,
                              String schemaKeyByParquetThreadID,
                              PackageReturnVo packageReturnVo,
                              int currentCount) throws Exception {

        log.info("[IceBergPool][tid={}] batch threshold reached, flush key={} count={} ops={}", Thread.currentThread().getId(), schemaKeyByParquetThreadID, currentCount, GlobalSetConfInfo.IceBergSchemaImmuTableOpsMap.get(schemaKeyByParquetThreadID).size());

        IceBergBatchOperationHandler.flushAllThreadsForTable(schemaKeyByParquet);

        Constant.writeToIceBergDBFlag.set(true);
        GlobalSetConfInfo.IceBergOperationCompleteMap.clear();
        GlobalSetConfInfo.IceBergOperationBeginMap.clear();

        log.info("[IceBergPool][tid={}] batch flush done key={}", Thread.currentThread().getId(), schemaKeyByParquetThreadID);
    }

    private void waitForAllThreadsComplete() throws InterruptedException {
        while (true) {
            for (Map.Entry<String, Boolean> entry :
                    GlobalSetConfInfo.IceBergOperationCompleteMap.entrySet()) {
                log.info("[IceBergPool][tid={}] waiting key={} done={} [{}/{}]", Thread.currentThread().getId(), entry.getKey(), entry.getValue(), GlobalSetConfInfo.IceBergOperationBeginMap.size(), GlobalSetConfInfo.IceBergOperationCompleteMap.size());
            }
            if (GlobalSetConfInfo.IceBergOperationBeginMap.size()
                    == GlobalSetConfInfo.IceBergOperationCompleteMap.size()) {
                break;
            }
            Thread.sleep(1000);
        }
    }

    // =========================================================================
    // 类型转换核心方法
    // =========================================================================

    /**
     * 核心类型转换：将 CDC 层传来的 String 转为 Iceberg GenericParquetWriter 期望的 Java 类型。
     * <p>
     * Iceberg 类型 → Java 类型对应关系：
     *   StringType    → String
     *   BooleanType   → Boolean
     *   LongType      → Long
     *   IntegerType   → Integer
     *   DecimalType   → BigDecimal
     *   TimestampType.withoutZone → LocalDateTime
     *   TimestampType.withZone    → OffsetDateTime
     *   BinaryType    → ByteBuffer（来源为十六进制或 Base64）
     */
    private Object convertValue(String rawValue,
                                org.apache.iceberg.types.Type fieldType,
                                String colName) {
        if (rawValue == null || rawValue.isEmpty()) return null;
        try {
            if (fieldType instanceof org.apache.iceberg.types.Types.StringType) {
                return rawValue;

            } else if (fieldType instanceof org.apache.iceberg.types.Types.BooleanType) {
                // Oracle NUMBER(1,0)：1/true → true，0/false → false
                return "1".equals(rawValue.trim()) || "true".equalsIgnoreCase(rawValue.trim()) || "t".equalsIgnoreCase(rawValue.trim());

            } else if (fieldType instanceof org.apache.iceberg.types.Types.LongType) {
                // Oracle NUMBER(n,0)；CDC 有时带小数点如 "12345.0" 或科学计数法
                String trimmed = rawValue.trim();
                try {
                    return Long.parseLong(trimmed);
                } catch (NumberFormatException e1) {
                    try {
                        // 处理 "12345.0" 或 "1.23E+10" 等形式
                        return new java.math.BigDecimal(trimmed).longValueExact();
                    } catch (Exception e2) {
                        // 最后兜底：截掉小数部分
                        int dotIdx = trimmed.indexOf('.');
                        if (dotIdx >= 0) trimmed = trimmed.substring(0, dotIdx);
                        return Long.parseLong(trimmed);
                    }
                }

            } else if (fieldType instanceof org.apache.iceberg.types.Types.IntegerType) {
                return Integer.parseInt(rawValue.trim());

            } else if (fieldType instanceof org.apache.iceberg.types.Types.DecimalType) {
                // Oracle NUMBER(%d,%d) / binary_float / binary_double
                org.apache.iceberg.types.Types.DecimalType decType =
                        (org.apache.iceberg.types.Types.DecimalType) fieldType;
                return new java.math.BigDecimal(rawValue.trim())
                        .setScale(decType.scale(), java.math.RoundingMode.HALF_UP);

            }  else if (fieldType instanceof org.apache.iceberg.types.Types.FloatType) {
            // BINARY_FLOAT：CDC 传来科学计数法字符串如 "3.40282E+38"，直接 parseFloat
            return Float.parseFloat(rawValue.trim());

             } else if (fieldType instanceof org.apache.iceberg.types.Types.DoubleType) {
                // BINARY_DOUBLE：同上，用 parseDouble
                return Double.parseDouble(rawValue.trim());
            } else if (fieldType instanceof org.apache.iceberg.types.Types.DateType) {
                // Oracle DATE 只含日期部分，对应 Java LocalDate
                String trimmed = rawValue.trim();
                // 有些 Oracle DATE 带时分秒，截掉只取日期部分
                if (trimmed.length() > 10 && trimmed.charAt(10) == ' ') {
                    trimmed = trimmed.substring(0, 10);
                }
                try { return java.time.LocalDate.parse(trimmed); } catch (Exception ignored) {}           // yyyy-MM-dd
                try { return java.time.LocalDate.parse(trimmed, DATE_FMT_SLASH); } catch (Exception ignored) {} // yyyy/MM/dd
                log.warn("[IceBergPool][tid={}] cannot parse date '{}' col={}, returning null",
                        Thread.currentThread().getId(), rawValue, colName);
                return null;
            } else if (fieldType instanceof org.apache.iceberg.types.Types.TimeType) {
                String trimmed = rawValue.trim();
                try {
                    return java.time.LocalTime.parse(trimmed, TIME_FMT_MICROSECOND);
                } catch (Exception ignored) {
                }
                try {
                    return java.time.LocalTime.parse(trimmed, TIME_FMT_MILLISECOND);
                } catch (Exception ignored) {
                }
                try {
                    return java.time.LocalTime.parse(trimmed);
                } catch (Exception ignored) {
                }
                log.warn("[IceBergPool][tid={}] cannot parse time '{}' col={}, returning null",
                        Thread.currentThread().getId(), rawValue, colName);
                return null;
            }else if (fieldType instanceof org.apache.iceberg.types.Types.TimestampType) {
                    boolean withZone = ((org.apache.iceberg.types.Types.TimestampType) fieldType).shouldAdjustToUTC();
                    return parseTimestampValue(rawValue.trim(), withZone);

            } else if (fieldType instanceof org.apache.iceberg.types.Types.BinaryType) {
                // 按要求统一降级为 string 处理
                return rawValue;

            } else {
                // FloatType、DoubleType 等其他未列举类型兜底
                return rawValue;
            }
        } catch (Exception e) {
            log.warn("[IceBergPool][tid={}] convertValue failed col={} type={} val='{}' err={}",
                    Thread.currentThread().getId(), colName, fieldType, rawValue, e.getMessage());
            return null;
        }
    }


    private Object parseTimestampValue(String raw, boolean withZone) {
        if (withZone) {
            try { return java.time.OffsetDateTime.parse(raw); } catch (Exception ignored) {}
            for (java.time.format.DateTimeFormatter fmt : ZONED_FORMATTERS) {
                try { return java.time.OffsetDateTime.parse(raw, fmt); } catch (Exception ignored) {}
            }
            java.time.LocalDateTime ldt = parseLocalDateTimeValue(raw);
            return ldt != null ? ldt.atOffset(java.time.ZoneOffset.UTC) : null;
        } else {
            return parseLocalDateTimeValue(raw);
        }
    }

    // =====================================================================
    // 【时间格式配置区】在此数组里添加/修改。
    //
    // 规则：
    //   1. 按"从精度高到低"排列，避免短格式误匹配长字符串。
    //   2. 纯日期格式（无时分秒，即 pattern 中不含 "H"）放在最后，
    //      匹配后自动补 00:00:00。
    //   3. 新增格式直接在数组末尾追加，格式串遵循 DateTimeFormatter 语法。
    //
    // 当前已支持格式（与 CDC/LogMiner 默认输出对齐）：
    //    "2024-01-15 12:34:56.123456789"  → yyyy-MM-dd HH:mm:ss.SSSSSSSSS （纳秒，9位）
    //    "2024-01-15 12:34:56.123456"     → yyyy-MM-dd HH:mm:ss.SSSSSS    （微秒，6位，Oracle TIMESTAMP 默认）
    //    "2024-01-15 12:34:56.123"        → yyyy-MM-dd HH:mm:ss.SSS       （毫秒）
    //    "2024-01-15 12:34:56"            → yyyy-MM-dd HH:mm:ss
    //    "2024-01-15 12:34"               → yyyy-MM-dd HH:mm
    //    "2024-01-15"                     → yyyy-MM-dd                    （纯日期，补 00:00:00）
    //    "2024/01/15 12:34:56"            → yyyy/MM/dd HH:mm:ss
    //    "2024/01/15"                     → yyyy/MM/dd                    （纯日期，补 00:00:00）
    //
    //  如果改了格式，或需新增，在下面数组追加一行即可，不要改其他代码。
    // =====================================================================
/*    private static final String[] TIMESTAMP_PATTERNS = {
            "yyyy-MM-dd HH:mm:ss.SSSSSSSSS",   // 纳秒（9位小数），LogMiner 高精度输出
            "yyyy-MM-dd HH:mm:ss.SSSSSS",      // 微秒（6位小数），Oracle TIMESTAMP 默认精度
            "yyyy-MM-dd HH:mm:ss.SSS",         // 毫秒（3位小数）
            "yyyy-MM-dd HH:mm:ss",             // 标准无小数
            "yyyy-MM-dd HH:mm",                // 无秒
            "yyyy-MM-dd",                      // 纯日期，自动补 00:00:00
            "yyyy/MM/dd HH:mm:ss",             // 斜线分隔带时间
            "yyyy/MM/dd",                      // 斜线分隔纯日期，自动补 00:00:00
            // "MM/dd/yyyy HH:mm:ss",          // 示例：美式日期，按需启用
    };*/


    private java.time.LocalDateTime parseLocalDateTimeValue(String raw) {
        for (int i = 0; i < TIMESTAMP_FORMATTERS.length; i++) {
            try {
                if (TIMESTAMP_IS_DATE_ONLY[i]) {
                    return java.time.LocalDate.parse(raw, TIMESTAMP_FORMATTERS[i]).atStartOfDay();
                }
                return java.time.LocalDateTime.parse(raw, TIMESTAMP_FORMATTERS[i]);
            } catch (Exception ignored) {}
        }
        log.warn("[IceBergPool][tid={}] cannot parse timestamp '{}', returning null",
                Thread.currentThread().getId(), raw);
        return null;
    }

}