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

        StringBuilder debugColLogs = null;
        if (Constant.debugLogEnabled) {
            debugColLogs = new StringBuilder(1024);
        }

        for (int rowNo = 0; rowNo < rowsNum; rowNo++) {

            GenericRecord newRecord = GenericRecord.create(
                    GlobalSetConfInfo.IceBergSchemaCahceMap.get(schemaKeyByParquet));
            GenericRecord oldRecord = GenericRecord.create(
                    GlobalSetConfInfo.IceBergSchemaCahceMap.get(schemaKeyByParquet));

            boolean hasNewData = false;
            boolean hasOldData = false;
            int mergerColLimit = columnsNum;

            if (Constant.debugLogEnabled) {
                debugColLogs.setLength(0);
            }

            for (int colNo = 0; colNo < columnsNum; colNo++) {
                Udb_BcolumnVo columnInfo = rowUdbColumnMap.get(rowNo + "-" + colNo);
                colNameByNumberKey = schemaKeyByParquet + "." + columnInfo.getColumnName().toLowerCase();
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

                int cflag = columnInfo.getCflag();
                if ((cflag & 1) == 1) {
                    // before image
                    fillRecord(oldRecord, columnInfo, colNameByNumberKey, opType);
                    hasOldData = true;
                } else {
                    // after image
                    fillRecord(newRecord, columnInfo, colNameByNumberKey, opType);
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

    private void fillRecord(GenericRecord record,
                            Udb_BcolumnVo columnInfo,
                            String colNameByNumberKey,
                            String opType) {
        try {
            if (columnInfo.getColumnValue() != null && !columnInfo.getColumnValue().equals("")) {
                record.setField(columnInfo.getColumnName().toLowerCase(), columnInfo.getColumnValue());
            }
        } catch (Exception e) {
            log.warn("[IceBergPool][tid={}] fillRecord failed col={} val={} err={}", Thread.currentThread().getId(), columnInfo.getColumnName(), columnInfo.getColumnValue(), e.getMessage());
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

}