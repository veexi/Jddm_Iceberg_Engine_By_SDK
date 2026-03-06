package com.jddm.thread;

import com.dsg.analysis.utils.ConversionUtil;
import com.dsg.analysis.vo.PackageReturnVo;
import com.dsg.analysis.vo.Udb_BcolumnVo;
import com.jddm.common.Constant;
import com.jddm.conf.GlobalConfInfo;
import com.jddm.conf.GlobalSetConfInfo;
import com.jddm.vo.RowOperation;
import com.jddm.operation.IceBergBatchOperationHandler;
import com.publics.common.ConstantPubSet;
import com.publics.common.ConstantPublic;
import com.publics.conf.GlobalConfCommInfo;
import org.apache.iceberg.data.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
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

                // DEBUG 模式：逐列打印原始 CDC 数据
                if (Constant.debugLogEnabled) {
                    log.info("[IceBergPool] ========== CDC Packet Info ==========");
                    log.info("[IceBergPool] pktSeq={} table={} opType={} rows={} cols={}",
                            pktSeq, schemaKeyByParquet, packageReturnVo.getOperationType(), rowsNum, columnsNum);
                    for (Map.Entry<String, Udb_BcolumnVo> entry : rowUdbColumnMap.entrySet()) {
                        Udb_BcolumnVo col = entry.getValue();
                        log.info("[IceBergPool] colData: key={} | name={} | value={} | cflag={}",
                                entry.getKey(), col.getColumnName(), col.getColumnValue(), col.getCflag());
                    }
                    log.info("[IceBergPool] ========== End Packet Info ==========");
                }

                if (GlobalSetConfInfo.IceBergSchemaCahceMap.get(schemaKeyByParquet) == null
                        || !GlobalSetConfInfo.IceBergCacheTableMap.containsKey(schemaKeyByParquet)) {
                    log.warn("[IceBergPool] table {} schema not loaded, skip packet", schemaKeyByParquet);
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
                log.info("[IceBergPool] pktSeq={} table={} op={} parsedOps={}{}",
                        pktSeq, schemaKeyByParquet, opType, opsBuffer.size(),
                        Constant.debugLogEnabled ? " opDetails=" + summarizeOps(opsBuffer, schemaKeyByParquet, 10) : "");

                GlobalSetConfInfo.IceBergSchemaImmuTableOpsMap
                        .get(schemaKeyByParquetThreadID)
                        .addAll(opsBuffer);

                GlobalConfInfo.lastDataWriteTimerByParquetMap
                        .put(schemaKeyByParquetThreadID, System.currentTimeMillis());

                int currentCount = GlobalConfInfo.engineAtomicByTableKeyMap
                        .get(schemaKeyByParquetThreadID).get();
                if (currentCount % Constant.writeCountNoToHiveFile == 0) {
                    triggerFlush(schemaKeyByParquet, schemaKeyByParquetThreadID, packageReturnVo, currentCount);
                }

            } catch (Exception ex) {
                ex.printStackTrace();
                log.error("[IceBergPool] thread exit on error. threadId={}", threadID, ex);
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

        if (("U".equals(opType) || "M".equals(opType)) && rowsNum == 2) {
            GenericRecord newRecord = GenericRecord.create(
                    GlobalSetConfInfo.IceBergSchemaCahceMap.get(schemaKeyByParquet));
            GenericRecord oldRecord = GenericRecord.create(
                    GlobalSetConfInfo.IceBergSchemaCahceMap.get(schemaKeyByParquet));
            boolean hasNew = false, hasOld = false;
            for (int rowNo = 0; rowNo < 2; rowNo++) {
                for (int colNo = 0; colNo < columnsNum; colNo++) {
                    Udb_BcolumnVo columnInfo = rowUdbColumnMap.get(rowNo + "-" + colNo);
                    colNameByNumberKey = schemaKeyByParquet + "." + columnInfo.getColumnName().toLowerCase();
                    if (columnInfo.getColumnName().equals(ConstantPubSet.MergerColKeyName)) {
                        continue;
                    }
                    if ((columnInfo.getCflag() & 1) == 1) {
                        fillRecord(oldRecord, columnInfo, colNameByNumberKey, "I");
                        hasOld = true;
                    } else {
                        fillRecord(newRecord, columnInfo, colNameByNumberKey, "I");
                        hasNew = true;
                    }
                }
            }
            if (hasNew && hasOld) {
                List<RowOperation> ops = buildOperation("U", newRecord, oldRecord, true, true, baseOffset, schemaKeyByParquet);
                if (ops != null) result.addAll(ops);
                return result;
            }
        }

        for (int rowNo = 0; rowNo < rowsNum; rowNo++) {

            GenericRecord newRecord = GenericRecord.create(
                    GlobalSetConfInfo.IceBergSchemaCahceMap.get(schemaKeyByParquet));
            GenericRecord oldRecord = GenericRecord.create(
                    GlobalSetConfInfo.IceBergSchemaCahceMap.get(schemaKeyByParquet));

            boolean hasNewData = false;
            boolean hasOldData = false;

            for (int colNo = 0; colNo < columnsNum; colNo++) {
                Udb_BcolumnVo columnInfo = rowUdbColumnMap.get(rowNo + "-" + colNo);
                colNameByNumberKey = schemaKeyByParquet + "." + columnInfo.getColumnName().toLowerCase();
                if(columnInfo.getColumnName().equals(ConstantPubSet.MergerColKeyName)) {

                    switch(columnInfo.getColumnValue().toUpperCase()) {

                        case "I":
                            opType="I";
                            break;
                        case "U":
                        case "UA":
                        case "UB":
                            opType="U";
                            break;
                        case "D":
                            opType="D";
                            break;
                    }
                    continue;
                }
                switch (opType) {
                    case "I":
                        fillRecord(newRecord, columnInfo, colNameByNumberKey, "I");
                        hasNewData = true;
                        break;

                    case "U":
                        if ((columnInfo.getCflag() & 1) == 1) {
                            fillRecord(oldRecord, columnInfo, colNameByNumberKey, "I");
                            hasOldData = true;
                        } else {
                            fillRecord(newRecord, columnInfo, colNameByNumberKey, "I");
                            hasNewData = true;
                        }
                        break;

                    case "D":
                        // DELETE操作：cflag=1或5表示前镜像(old)，其他表示后镜像(new)
                        // 对于DELETE，需要同时填充oldRecord和newRecord以保留主键信息
                        if ((columnInfo.getCflag() & 1) == 1) {
                            fillRecord(oldRecord, columnInfo, colNameByNumberKey, "I");
                            hasOldData = true;
                        } else {
                            fillRecord(newRecord, columnInfo, colNameByNumberKey, "I");
                            hasNewData = true;
                        }
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
            log.info("[IceBergPool] buildOperation opType={} txMode={} hasNewData={} hasOldData={} offset={}",
                    opType, isTransaction, hasNewData, hasOldData, offset);
        }

        switch (opType) {
            case "U":
                if (isTransaction && hasOldData && hasNewData) {
                    return Collections.singletonList(RowOperation.update(oldRecord, newRecord, offset));
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
                GenericRecord deleteRecord = chooseDeleteRecordByPk(tableKeyName, oldRecord, newRecord, hasOldData, hasNewData);
                if (isTransaction && (hasNewData || hasOldData)) {
                    return Collections.singletonList(RowOperation.delete(deleteRecord, offset));
                }
                return Collections.singletonList(RowOperation.insert(deleteRecord, offset));
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
        if (pkNames != null && !pkNames.isEmpty()) {
            boolean oldPkReady = hasOldData && isPkReady(oldRecord, pkNames);
            boolean newPkReady = hasNewData && isPkReady(newRecord, pkNames);
            if (oldPkReady) {
                if (Constant.debugLogEnabled) {
                    log.info("[IceBergPool] delete choose OLD by PK table={} pkNames={}", tableKeyName, pkNames);
                }
                return oldRecord;
            }
            if (newPkReady) {
                if (Constant.debugLogEnabled) {
                    log.info("[IceBergPool] delete choose NEW by PK table={} pkNames={}", tableKeyName, pkNames);
                }
                return newRecord;
            }
            log.warn("[IceBergPool] delete PK missing on both sides, fallback table={} pkNames={}", tableKeyName, pkNames);
        }
        return hasOldData ? oldRecord : newRecord;
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
            if (GlobalConfCommInfo.jddmEngineTypeByYloaderColMap.containsKey(colNameByNumberKey)) {
                if (columnInfo.getColumnValue() != null && !columnInfo.getColumnValue().equals("")) {
                    record.setField(columnInfo.getColumnName().toLowerCase(), columnInfo.getColumnValue());
                }
                return;
            }

            switch (columnInfo.getColTypeArr()[1]) {

                case 0x02: // NUMBER
                    if (columnInfo.getColumnValue() == null || columnInfo.getColumnValue().equals("")) {
                        record.setField(columnInfo.getColumnName().toLowerCase(), null);
                    } else if (GlobalConfCommInfo.jddmEngineTypeByNumberColMap.containsKey(colNameByNumberKey)) {
                        switch (GlobalConfCommInfo.jddmEngineTypeByNumberColMap.get(colNameByNumberKey)) {
                            case 1000: case 1100: case 1200:
                                record.setField(columnInfo.getColumnName().toLowerCase(), columnInfo.getColumnValue());
                                break;
                            case 3000:
                                record.setField(columnInfo.getColumnName().toLowerCase(),
                                        Double.parseDouble(columnInfo.getColumnValue()));
                                break;
                            case 3100:
                                record.setField(columnInfo.getColumnName().toLowerCase(), columnInfo.getColumnValue());
                                break;
                        }
                    }
                    break;

                case -75: case -76: // TIMESTAMP
                    if (columnInfo.getColumnValue() == null || columnInfo.getColumnValue().equals("")) {
                        record.setField(columnInfo.getColumnName().toLowerCase(), null);
                    } else {
                        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
                        record.setField(columnInfo.getColumnName().toLowerCase(),
                                LocalDateTime.parse(columnInfo.getColumnValue(), fmt));
                    }
                    break;

                case 0x64: case 0x65: // Binary_Float / Binary_Double
                    if (columnInfo.getColumnValue() != null && !columnInfo.getColumnValue().equals("")) {
                        record.setField(columnInfo.getColumnName().toLowerCase(),
                                Double.parseDouble(columnInfo.getColumnValue()));
                    }
                    break;

                case 0x70: case 0x71:
                    if (columnInfo.getColumnValue() != null && !columnInfo.getColumnValue().equals("")) {
                        if (GlobalConfCommInfo.jddmEngineTypeBy0x71BytesColMap.containsKey(colNameByNumberKey)) {
                            record.setField(columnInfo.getColumnName().toLowerCase(), columnInfo.getColumnValue());
                        } else {
                            record.setField(columnInfo.getColumnName().toLowerCase(),
                                    new String(ConversionUtil.hexStringsToBytes(columnInfo.getColumnValue())));
                        }
                    }
                    break;

                default:
                    if (columnInfo.getColumnValue() != null && !columnInfo.getColumnValue().equals("")) {
                        record.setField(columnInfo.getColumnName().toLowerCase(), columnInfo.getColumnValue());
                    }
                    break;
            }
        } catch (Exception e) {
            log.warn("[IceBergPool] fillRecord failed col={} val={} err={}",
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
                .putIfAbsent(schemaKeyByParquetThreadID, new ArrayList<>());
        log.info("[IceBergPool] cache init done key={} cost={}ms",
                schemaKeyByParquetThreadID, System.currentTimeMillis() - startTimer);
    }

    private void triggerFlush(String schemaKeyByParquet,
                              String schemaKeyByParquetThreadID,
                              PackageReturnVo packageReturnVo,
                              int currentCount) throws Exception {

        log.info("[IceBergPool] batch threshold reached, flush key={} count={} ops={}",
                schemaKeyByParquetThreadID, currentCount,
                GlobalSetConfInfo.IceBergSchemaImmuTableOpsMap.get(schemaKeyByParquetThreadID).size());

/*        DataFileToIceBergOperationV1 dataFileToIceBergOperation = new DataFileToIceBergOperationV1();
        dataFileToIceBergOperation.setSchemaKeyByParquet(schemaKeyByParquet);
        dataFileToIceBergOperation.setSchemaKeyByParquetThreadID(schemaKeyByParquetThreadID);
        dataFileToIceBergOperation.run();*/
        IceBergBatchOperationHandler.flushAllThreadsForTable(schemaKeyByParquet);


        GlobalConfInfo.lastDataWriteTimerByParquetMap
                .put(schemaKeyByParquetThreadID, System.currentTimeMillis());

//        waitForAllThreadsComplete();

        Constant.writeToIceBergDBFlag = true;
        GlobalSetConfInfo.IceBergOperationCompleteMap.clear();
        GlobalSetConfInfo.IceBergOperationBeginMap.clear();

        log.info("[IceBergPool] batch flush done key={}", schemaKeyByParquetThreadID);
    }

    private void waitForAllThreadsComplete() throws InterruptedException {
        while (true) {
            for (Map.Entry<String, Boolean> entry :
                    GlobalSetConfInfo.IceBergOperationCompleteMap.entrySet()) {
                log.info("[IceBergPool] waiting key={} done={} [{}/{}]",
                        entry.getKey(), entry.getValue(),
                        GlobalSetConfInfo.IceBergOperationBeginMap.size(),
                        GlobalSetConfInfo.IceBergOperationCompleteMap.size());
            }
            if (GlobalSetConfInfo.IceBergOperationBeginMap.size()
                    == GlobalSetConfInfo.IceBergOperationCompleteMap.size()) {
                break;
            }
            Thread.sleep(1000);
        }
    }

}