package com.jddm.thread;


import com.dsg.analysis.vo.PackageReturnVo;
import com.dsg.analysis.vo.Udb_BcolumnVo;
import com.jddm.common.Constant;
import com.jddm.conf.GlobalConfInfo;
import com.jddm.conf.GlobalSetConfInfo;
import com.jddm.vo.RowOperation;
import com.jddm.operation.IceBergBatchOperationHandler;
import com.publics.common.ConstantPubSet;
import com.publics.common.ConstantPublic;

import org.apache.iceberg.data.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
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
        Object recvPackageObj;
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

                recvPackageObj = GlobalSetConfInfo.icebergEngineOperationQueue.take();
                if (!(recvPackageObj instanceof PackageReturnVo)) {
                    continue;
                }

                packageReturnVo   = (PackageReturnVo) recvPackageObj;
                long pktSeq      = GlobalConfInfo.icebergEngineOperationSeq.getAndIncrement();
                rowUdbColumnMap   = packageReturnVo.getRowUdbColumnMap();
                rowsNum           = Integer.parseInt(packageReturnVo.getRowsCount());
                columnsNum        = Integer.parseInt(packageReturnVo.getColsCount());
                schemaKeyByParquet = packageReturnVo.getOwnerName().toLowerCase()
                        + "." + packageReturnVo.getTableName().toLowerCase();
                schemaKeyByParquetThreadID = schemaKeyByParquet + "." + threadID;

// Debug logs for columns have been integrated into the main parsing loop below to prevent OOM

                if (GlobalSetConfInfo.IceBergSchemaCahceMap.get(schemaKeyByParquet) == null
                        || !GlobalSetConfInfo.IceBergCacheTableMap.containsKey(schemaKeyByParquet)) {
                    log.warn("[IceBergPool][tid={}] table {} schema not loaded, skip packet", Thread.currentThread().getId(), schemaKeyByParquet);
                    continue;
                }

                GlobalConfInfo.engineAtomicByTableKeyMap
                        .computeIfAbsent(schemaKeyByParquetThreadID, k -> new AtomicInteger(0))
                        .getAndIncrement();

                initCacheIfAbsent(schemaKeyByParquet, schemaKeyByParquetThreadID);

                String opType = packageReturnVo.getOperationType().toUpperCase();
                List<RowOperation> opsBuffer = buildRowOperations(
                        opType, rowsNum, columnsNum, rowUdbColumnMap,
                        schemaKeyByParquet, schemaKeyByParquetThreadID, colNameByNumberKey,
                        pktSeq);
                log.info("[IceBergPool][tid={}] pktSeq={} table={} op={} parsedOps={}", Thread.currentThread().getId(), pktSeq, schemaKeyByParquet, opType, opsBuffer.size());
                if (Constant.debugLogEnabled) {
                    log.info("[IceBergPool][tid={}] pktSeq={} opDetails={}", Thread.currentThread().getId(), pktSeq, summarizeOps(opsBuffer, schemaKeyByParquet, 10));
                }

                GlobalSetConfInfo.IceBergSchemaImmuTableOpsMap
                        .get(schemaKeyByParquetThreadID)
                        .addAll(opsBuffer);

                GlobalConfInfo.lastDataWriteTimerByParquetMap
                        .put(schemaKeyByParquetThreadID, System.currentTimeMillis());

                int currentCount = GlobalConfInfo.engineAtomicByTableKeyMap
                        .computeIfAbsent(schemaKeyByParquetThreadID, k -> new AtomicInteger(0))
                        .addAndGet(rowsNum);

                if (currentCount >= Constant.writeCountNoToHiveFile) {
                    log.info("[IcebergPool][tid={}] count rows ::  {}>={}", Thread.currentThread().getId(), currentCount, Constant.writeCountNoToHiveFile);
                    triggerFlush(schemaKeyByParquet, schemaKeyByParquetThreadID, packageReturnVo, currentCount);
                    // 提交后立刻重置该线程的计数器
//                    GlobalConfInfo.engineAtomicByTableKeyMap.get(schemaKeyByParquetThreadID).set(0);
                }else {
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
     */
    private List<RowOperation> buildRowOperations(
            String opType,
            int rowsNum,
            int columnsNum,
            Map<String, Udb_BcolumnVo> rowUdbColumnMap,
            String schemaKeyByParquet,
            String schemaKeyByParquetThreadID,
            String colNameByNumberKey,
            long pktSeq) {

        List<RowOperation> result = new ArrayList<>(rowsNum);
        long baseOffset = pktSeq * 1_000_000L;

        for (int rowNo = 0; rowNo < rowsNum; rowNo++) {

            GenericRecord newRecord = GenericRecord.create(
                    GlobalSetConfInfo.IceBergSchemaCahceMap.get(schemaKeyByParquet));
            GenericRecord oldRecord = GenericRecord.create(
                    GlobalSetConfInfo.IceBergSchemaCahceMap.get(schemaKeyByParquet));

            boolean hasNewData = false;
            boolean hasOldData = false;
            int mergerColLimit = columnsNum;  // merge I/D 只处理前半列

            for (int colNo = 0; colNo < columnsNum; colNo++) {
                Udb_BcolumnVo columnInfo = rowUdbColumnMap.get(rowNo + "-" + colNo);
                colNameByNumberKey = schemaKeyByParquet + "." + columnInfo.getColumnName().toLowerCase();
                if(columnInfo.getColumnName().equals(ConstantPubSet.MergerColKeyName)) {

                    switch(columnInfo.getColumnValue().toUpperCase()) {

                        case "I":
                            opType="I";
                            mergerColLimit = columnsNum / 2;
                            break;
                        case "U":
                        case "UA":
                        case "UB":
                            opType="U";
                            break;
                        case "D":
                            opType="D";
                             mergerColLimit = columnsNum / 2;
                            break;
                    }
                    continue;
                }
                // merge模式下 I/D 只有前半列有效数据，跳过后半空列
                if (colNo > mergerColLimit) {
                    continue;
                }
                
                // 将原本独立的 Debug 打印移入主循环，减少 OOM 风险和性能开销
                if (Constant.debugLogEnabled) {
                    log.info("[IceBergPool][tid={}] table={} op={} row={} colName={} colValue={} cflag={}",
                            Thread.currentThread().getId(), schemaKeyByParquet, opType, rowNo,
                            columnInfo.getColumnName(), columnInfo.getColumnValue(), columnInfo.getCflag());
                }
                switch (opType) {
                    case "I":
                        if ((columnInfo.getCflag() & 1) == 1) {
                            break;
                        }
                        fillRecord(newRecord, columnInfo, colNameByNumberKey, "I");
                        hasNewData = true;
                        break;

                    case "U":
                        if ((columnInfo.getCflag() & 1) > 0) {
                            fillRecord(oldRecord, columnInfo, colNameByNumberKey, "I");
                            hasOldData = true;
                        } else {
                            fillRecord(newRecord, columnInfo, colNameByNumberKey, "I");
                            hasNewData = true;
                        }
                        break;

                    case "D":
                        // DELETE操作：强制全量放入 oldRecord
                        fillRecord(oldRecord, columnInfo, colNameByNumberKey, "I");
                        hasOldData = true;
                        break;
                }
            }

            List<RowOperation> ops = buildOperation(opType, newRecord, oldRecord,
                    hasNewData, hasOldData, baseOffset + rowNo, schemaKeyByParquet);
            if (ops != null) {
                result.addAll(ops);
            }
        }

        return result;
    }

    private List<RowOperation> buildOperation(String opType,
                                              GenericRecord newRecord,
                                              GenericRecord oldRecord,
                                              boolean hasNewData,
                                              boolean hasOldData,
                                              long offset,
                                              String tableKeyName) {
        boolean isTransaction = "transaction".equals(Constant.icebergWriteMode);
        if (Constant.debugLogEnabled) {
            log.info("[IceBergPool][tid={}] buildOperation table={} opType={} txMode={} hasNewData={} hasOldData={} offset={}", Thread.currentThread().getId(), tableKeyName, opType, isTransaction, hasNewData, hasOldData, offset);
        }

        boolean hasPk = hasPk(tableKeyName);

        switch (opType) {
            case "U":
                if (isTransaction && hasPk) {
                    if (hasOldData && hasNewData) {
                        // 判断主键是否发生了变更
                        List<String> pkNames = GlobalSetConfInfo.TablePkColCacheMap.get(tableKeyName);
                        String oldPkString = extractRecordKey(oldRecord, pkNames);
                        String newPkString = extractRecordKey(newRecord, pkNames);

                        if (!oldPkString.equals(newPkString)) {
                            if (Constant.debugLogEnabled) {
                                log.info("[IceBergPool][tid={}] UPDATE changed PK table={}: {} -> {}, splitting into DELETE + INSERT", Thread.currentThread().getId(), tableKeyName, oldPkString, newPkString);
                            }
                            // 主键发生变更，拆分为先删后插
                            List<RowOperation> splitOps = new ArrayList<>(2);
                            splitOps.add(RowOperation.delete(oldRecord, offset));
                            splitOps.add(RowOperation.insert(newRecord, offset + 1));
                            return splitOps;
                        }

                        // 主键未变更，标准双镜像 UPDATE
                        return Collections.singletonList(RowOperation.update(oldRecord, newRecord, offset));
                    }
                    if (!hasOldData && hasNewData) {
                        // 问题2场景：CDC 只发后镜像的单行 UPDATE 包。
                        // 此时无前镜像，无法做 Equality Delete，改为 delete(new)+insert(new) 语义兜底：
                        // 先删后写，保证幂等，不产生重复行。
                        log.warn("[IceBergPool][tid={}] UPDATE only after-image, fallback delete+insert table={} offset={}", Thread.currentThread().getId(), tableKeyName, offset);
                        List<RowOperation> ops = new ArrayList<>(2);
                        ops.add(RowOperation.delete(newRecord, offset));
                        ops.add(RowOperation.insert(newRecord, offset + 1));
                        return ops;
                    }
                }
                // 无主键表 transaction 模式退化为轨迹模式（insert only），或 hasOldData/hasNewData 不完整时兜底
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
                return Collections.singletonList(RowOperation.insert(newRecord, offset));

            case "D":
                // 两侧都没有数据（极端异常包），直接跳过，绝不写入空 record 产生 NULL 行
                if (!hasOldData && !hasNewData) {
                    log.error("[IceBergPool][tid={}] DELETE skip: no data on either side, table={} offset={}", Thread.currentThread().getId(), tableKeyName, offset);
                    return Collections.emptyList();
                }

                if (isTransaction && hasPk) {
                    // 事务模式 + 有主键：走 Equality Delete
                    GenericRecord deleteRecord = chooseDeleteRecordByPk(
                            tableKeyName, oldRecord, newRecord, hasOldData, hasNewData);
                    if (deleteRecord == null) {
                        log.error("[IceBergPool][tid={}] DELETE skip: PK fill failed, cannot locate row table={} offset={}", Thread.currentThread().getId(), tableKeyName, offset);
                        return Collections.emptyList();
                    }
                    return Collections.singletonList(RowOperation.delete(deleteRecord, offset));
                }

                // 轨迹模式（所有表）或事务模式无主键表：退化为 insert，保留 PK 感知选择逻辑
                if (!hasPk && Constant.debugLogEnabled) {
                    log.info("[IceBergPool][tid={}] DELETE degrade to trajectory (no PK) table={}", Thread.currentThread().getId(), tableKeyName);
                }
                // 优先取 PK 完整的前镜像，其次后镜像，最后按 hasOldData 兜底，与原 chooseDeleteRecordByPk 行为一致
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

    /**
     * 选取用于 DELETE 的目标 record。
     * 优先取前镜像（old），其次取后镜像（new）。
     * 若两侧 PK 均不完整（fillRecord 异常导致 PK 字段为 null），返回 null，
     * 调用方收到 null 后应跳过该 DELETE，避免按错误主键删行。
     *
     * <p>关于 setField 异常是否会丢数据：<br>
     * fillRecord 吞掉了 setField 异常，PK 字段保持 null。
     * isPkReady 检测到 null 返回 false，本方法返回 null，
     * buildOperation 跳过此次 DELETE 并打 ERROR 日志。<br>
     * 结果是：<b>该行不会被删除</b>（"漏删"而非"乱删"），数据不会因误删而丢失，
     * 但会留下一条本应消失的脏数据，需要通过日志排查 fillRecord 的类型转换问题。
     */
    private GenericRecord chooseDeleteRecordByPk(String tableKeyName,
                                                 GenericRecord oldRecord,
                                                 GenericRecord newRecord,
                                                 boolean hasOldData,
                                                 boolean hasNewData) {
        List<String> pkNames = GlobalSetConfInfo.TablePkColCacheMap.get(tableKeyName);
        if (pkNames == null || pkNames.isEmpty()) {
            // 无主键表，调用方已在 buildOperation 中用 hasPk() 拦截，此处不应到达
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

        // PK 两侧均不完整，极可能是 fillRecord 类型转换异常导致字段为 null
        // 返回 null → buildOperation 跳过本次 DELETE，避免按错误主键删行
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

    /** 判断该表是否有主键（无主键表 transaction 模式退化为轨迹模式） */
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
            // DELETE op: oldRecord holds the chosen delete record (may come from after-image)
            // Use same PK-aware fallback for display consistency
            GenericRecord record;
            if (op.getType() == RowOperation.OpType.DELETE) {
                record = op.getOldRecord() != null ? op.getOldRecord() : op.getNewRecord();
            } else {
                record = op.getNewRecord() != null ? op.getNewRecord() : op.getOldRecord();
            }
            out.add(op.getType() + "@" + op.getBinlogOffset() + "{"
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
            // 所有字段统一按 String 写入，不再做类型转换
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
                .putIfAbsent(schemaKeyByParquetThreadID, new LinkedBlockingQueue<>());
        log.info("[IceBergPool][tid={}] cache init done key={} cost={}ms", Thread.currentThread().getId(), schemaKeyByParquetThreadID, System.currentTimeMillis() - startTimer);
    }

    private void triggerFlush(String schemaKeyByParquet,
                              String schemaKeyByParquetThreadID,
                              PackageReturnVo packageReturnVo,
                              int currentCount) throws Exception {

        log.info("[IceBergPool][tid={}] batch threshold reached, flush key={} count={} ops={}", Thread.currentThread().getId(), schemaKeyByParquetThreadID, currentCount, GlobalSetConfInfo.IceBergSchemaImmuTableOpsMap.get(schemaKeyByParquetThreadID).size());

/*        DataFileToIceBergOperationV1 dataFileToIceBergOperation = new DataFileToIceBergOperationV1();
        dataFileToIceBergOperation.setSchemaKeyByParquet(schemaKeyByParquet);
        dataFileToIceBergOperation.setSchemaKeyByParquetThreadID(schemaKeyByParquetThreadID);
        dataFileToIceBergOperation.run();*/
        IceBergBatchOperationHandler.flushAllThreadsForTable(schemaKeyByParquet);


        GlobalConfInfo.lastDataWriteTimerByParquetMap
                .put(schemaKeyByParquetThreadID, System.currentTimeMillis());

//        waitForAllThreadsComplete();

        Constant.writeToIceBergDBFlag.set(true);   // 问题4修复：AtomicBoolean 保证多线程可见性
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