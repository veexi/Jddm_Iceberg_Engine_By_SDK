package com.jddm.operation;

import com.jddm.common.Constant;
import com.jddm.conf.GlobalConfInfo;
import com.jddm.conf.GlobalSetConfInfo;
import com.jddm.vo.RowOperation;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.*;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Batch handler: merge ops by PK, write data/delete files, commit RowDelta.
 */
public class IceBergBatchOperationHandler {

    private static final Logger log = LogManager.getLogger(IceBergBatchOperationHandler.class);

    /**
     * @param immuTableKeyName  e.g. db.table.threadId
     * @param tableKeyName      e.g. db.table
     * @param orderedOps        sorted by binlogOffset
     */
    public static void flushBatch(String immuTableKeyName,
                                  String tableKeyName,
                                  List<RowOperation> orderedOps) throws Exception {

        if (orderedOps == null || orderedOps.isEmpty()) {
            log.info("[IceBergBatch] empty batch, skip key={}", immuTableKeyName);
            return;
        }

        Table iceBergTable = GlobalSetConfInfo.IceBergCacheTableMap.get(tableKeyName);
        if (iceBergTable == null) {
            throw new IllegalStateException("[IceBergBatch] table not found: " + tableKeyName);
        }

        List<RowOperation> allOps = orderedOps;

        if (allOps.isEmpty()) {
            log.info("[IceBergBatch] ops empty, skip key={}", tableKeyName);
            return;
        }

        if ("transaction".equals(Constant.icebergWriteMode)) {
            List<String> pkNames = GlobalSetConfInfo.TablePkColCacheMap.get(tableKeyName);
            log.info("[IceBergBatch][TX] flush start table={} ops={} pkNames={}",
                    tableKeyName, allOps.size(), pkNames);
            MergeResult mergeResult = mergeByPrimaryKey(allOps, pkNames, iceBergTable);
            log.info("[IceBergBatch][TX] merge done table={} insertCount={} deleteCount={} insertKeys={} deleteKeys={}",
                    tableKeyName,
                    mergeResult.insertRecords.size(),
                    mergeResult.deleteRecords.size(),
                    summarizeRecords(mergeResult.insertRecords, pkNames, 10),
                    summarizeRecords(mergeResult.deleteRecords, pkNames, 10));
            DataFile dataFile = mergeResult.insertRecords.isEmpty() ? null
                    : writeDataFile(iceBergTable, mergeResult.insertRecords, tableKeyName);
            DeleteFile deleteFile = null;
            if (!mergeResult.deleteRecords.isEmpty()) {
                List<Integer> equalityFieldIds = resolveEqualityFieldIds(iceBergTable, pkNames);
                deleteFile = writeDeleteFile(iceBergTable, mergeResult.deleteRecords, equalityFieldIds);
            }
            commitRowDelta(iceBergTable, dataFile, deleteFile, tableKeyName);
            log.info("[IceBergBatch] transaction commit ok key={} inserts={} deletes={}",
                    tableKeyName, mergeResult.insertRecords.size(), mergeResult.deleteRecords.size());
        } else {
            List<GenericRecord> recordsToWrite = allOps.stream()
                    .map(op -> op.getNewRecord() != null ? op.getNewRecord() : op.getOldRecord())
                    .collect(Collectors.toList());
            DataFile dataFile = writeDataFile(iceBergTable, recordsToWrite, tableKeyName);
            iceBergTable.newAppend().appendFile(dataFile).commit();
            log.info("[IceBergBatch] trajectory commit ok key={} rows={}", tableKeyName, allOps.size());
        }
    }

    /**
     * Merge ops by PK, keep final state.
     */
    private static MergeResult mergeByPrimaryKey(List<RowOperation> orderedOps,
                                                 List<String> pkNames,
                                                 Table iceBergTable) {
        LinkedHashMap<String, RowOperation> mergedMap = new LinkedHashMap<>();

        if (Constant.debugLogEnabled) {
            log.info("[IceBergBatch] ===== mergeByPrimaryKey start, ops count={} =====", orderedOps.size());
        }

        for (RowOperation op : orderedOps) {
            String pk = extractPkKey(op, pkNames);
            RowOperation existing = mergedMap.get(pk);

            if (Constant.debugLogEnabled) {
                log.info("[IceBergBatch] Processing op: type={} pk={} offset={} hasNew={} hasOld={}",
                        op.getType(), pk, op.getBinlogOffset(),
                        op.getNewRecord() != null, op.getOldRecord() != null);
            }

            if (existing == null) {
                mergedMap.put(pk, op);
                if (Constant.debugLogEnabled) {
                    log.info("[IceBergBatch] -> new pk={}", pk);
                }
            } else {
                RowOperation folded = foldOperations(existing, op);
                if (folded == null) {
                    mergedMap.remove(pk);
                    if (Constant.debugLogEnabled) {
                        log.info("[IceBergBatch] -> cancelled pk={}", pk);
                    }
                } else {
                    mergedMap.put(pk, folded);
                    if (Constant.debugLogEnabled) {
                        log.info("[IceBergBatch] -> folded to type={} pk={}", folded.getType(), pk);
                    }
                }
            }
        }

        if (Constant.debugLogEnabled) {
            log.info("[IceBergBatch] ===== mergeByPrimaryKey end, mergedMap size={} =====", mergedMap.size());
            for (Map.Entry<String, RowOperation> entry : mergedMap.entrySet()) {
                log.info("[IceBergBatch] Final merged: pk={} -> type={}", entry.getKey(), entry.getValue().getType());
            }
        }

        List<GenericRecord> finalInserts = new ArrayList<>();
        List<GenericRecord> finalDeletes = new ArrayList<>();

        for (RowOperation op : mergedMap.values()) {
            switch (op.getType()) {
                case INSERT:
                    finalInserts.add(op.getNewRecord());
                    if (Constant.debugLogEnabled) {
                        log.info("[IceBergBatch] Final INSERT: {}", op.getNewRecord());
                    }
                    break;

                case DELETE:
                    if (op.getOldRecord() != null) {
                        finalDeletes.add(op.getOldRecord());
                        if (Constant.debugLogEnabled) {
                            log.info("[IceBergBatch] Final DELETE: {}", op.getOldRecord());
                        }
                    } else {
                        log.warn("[IceBergBatch] DELETE op has no record, skip. op={}", op);
                    }
                    break;

                case UPDATE:
                    if (op.getOldRecord() != null) {
                        finalDeletes.add(op.getOldRecord());
                        if (Constant.debugLogEnabled) {
                            log.info("[IceBergBatch] Final UPDATE del: {}", op.getOldRecord());
                        }
                    } else {
                        log.warn("[IceBergBatch] UPDATE op oldRecord null, insert only. op={}", op);
                    }
                    if (op.getNewRecord() != null) {
                        finalInserts.add(op.getNewRecord());
                        if (Constant.debugLogEnabled) {
                            log.info("[IceBergBatch] Final UPDATE ins: {}", op.getNewRecord());
                        }
                    }
                    break;

                default:
                    log.warn("[IceBergBatch] unknown op type: {}", op.getType());
            }
        }

        return new MergeResult(finalInserts, finalDeletes);
    }

    /**
     * Fold existing + incoming. Return null if mutually cancelled.
     */
    private static RowOperation foldOperations(RowOperation existing, RowOperation incoming) {
        RowOperation.OpType existType = existing.getType();
        RowOperation.OpType incomType = incoming.getType();
        if (Constant.debugLogEnabled) {
            log.info("[IceBergBatch] fold {}@{} + {}@{}",
                    existType, existing.getBinlogOffset(), incomType, incoming.getBinlogOffset());
        }

        if (existType == RowOperation.OpType.INSERT && incomType == RowOperation.OpType.INSERT) {
            log.warn("[IceBergBatch] dup INSERT same PK, use latter");
            return incoming;
        }

        if (existType == RowOperation.OpType.INSERT && incomType == RowOperation.OpType.UPDATE) {
            return RowOperation.insert(incoming.getNewRecord(), incoming.getBinlogOffset());
        }

        if (existType == RowOperation.OpType.INSERT && incomType == RowOperation.OpType.DELETE) {
            return null;
        }

        if (existType == RowOperation.OpType.UPDATE && incomType == RowOperation.OpType.UPDATE) {
            return RowOperation.update(existing.getOldRecord(), incoming.getNewRecord(), incoming.getBinlogOffset());
        }

        if (existType == RowOperation.OpType.UPDATE && incomType == RowOperation.OpType.DELETE) {
            return RowOperation.delete(existing.getOldRecord(), incoming.getBinlogOffset());
        }

        if (existType == RowOperation.OpType.DELETE && incomType == RowOperation.OpType.INSERT) {
            return RowOperation.update(existing.getOldRecord(), incoming.getNewRecord(), incoming.getBinlogOffset());
        }

        if (existType == RowOperation.OpType.DELETE && incomType == RowOperation.OpType.DELETE) {
            log.warn("[IceBergBatch] dup DELETE same PK, use latter");
            return incoming;
        }

        if (existType == RowOperation.OpType.DELETE && incomType == RowOperation.OpType.UPDATE) {
            return RowOperation.update(existing.getOldRecord(), incoming.getNewRecord(), incoming.getBinlogOffset());
        }

        log.warn("[IceBergBatch] unhandled fold: {} + {}", existType, incomType);
        return incoming;
    }

    public static DataFile writeDataFile(Table table,
                                         List<GenericRecord> records,
                                         String tableKeyName) throws Exception {
        Schema schema = table.schema();
        FileFormat format = FileFormat.PARQUET;
        OutputFile outputFile = table.io().newOutputFile(
                new Path(table.location(), "data/" + tableKeyName.replace(".", "/")
                        + "/" + UUID.randomUUID() + ".parquet").toString());

        FileAppender<GenericRecord> appender = Parquet.write(outputFile)
                .schema(schema)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .build();

        try (Closeable toClose = appender) {
            appender.addAll(records);
        }

        return DataFiles.builder(table.spec())
                .withInputFile(outputFile.toInputFile())
                .withMetrics(appender.metrics())
                .withFormat(format)
                .build();
    }

    private static DeleteFile writeDeleteFile(Table iceBergTable,
                                              List<GenericRecord> deleteRecords,
                                              List<Integer> equalityFieldIds) throws Exception {
        String deletePath = iceBergTable.location() + "/delete/eq_del_" + UUID.randomUUID();
        OutputFile deleteOut = iceBergTable.io().newOutputFile(deletePath);

        EqualityDeleteWriter<GenericRecord> deleteWriter = Parquet.writeDeletes(deleteOut)
                .forTable(iceBergTable)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .equalityFieldIds(equalityFieldIds)
                .buildEqualityWriter();
        try {
            for (GenericRecord rec : deleteRecords) {
                deleteWriter.write(rec);
            }
        } finally {
            deleteWriter.close();
        }

        log.debug("[IceBergBatch] DeleteFile written path={} rows={}", deletePath, deleteRecords.size());
        return deleteWriter.result().deleteFiles().get(0);
    }

    public static void flushAllThreadsForTable(String tableKeyName) throws Exception {
        List<RowOperation> allOps = new ArrayList<>();
        List<String> keysToRemove = new ArrayList<>();

        for (Map.Entry<String, java.util.concurrent.LinkedBlockingQueue<RowOperation>> entry :
                GlobalSetConfInfo.IceBergSchemaImmuTableOpsMap.entrySet()) {
            String key = entry.getKey();
            if (!key.startsWith(tableKeyName + ".")) continue;

            // drainTo 原子地把队列中现有数据全部取走，之后消费线程继续 offer 到同一个队列，互不干扰。
            // 若两个线程同时 flush 同一张表，第二个线程 drain 到空列表会在下面 isEmpty() 处直接 return，
            // 不会出现数据被写两次的情况。
            List<RowOperation> drained = new ArrayList<>();
            entry.getValue().drainTo(drained);
            if (!drained.isEmpty()) {
                allOps.addAll(drained);
                keysToRemove.add(key);
            }
        }

        if (allOps.isEmpty()) return;

        allOps.sort(Comparator.comparingLong(RowOperation::getBinlogOffset));
        flushBatch(tableKeyName, tableKeyName, allOps);

        // 只清 generic cache 和 timer，OpsMap 本身不删（队列留着继续接收新数据）
        keysToRemove.forEach(k -> {
            GlobalSetConfInfo.IceBergTableGnericCacheMap.remove(k);
            GlobalConfInfo.lastDataWriteTimerByParquetMap.remove(k);
        });
    }
    private static void commitRowDelta(Table iceBergTable,
                                       DataFile dataFile,
                                       DeleteFile deleteFile,
                                       String logKey) {

        int maxRetry = 3;
        int attempt  = 0;

        while (attempt < maxRetry) {
            attempt++;
            try {
                iceBergTable.refresh();

                RowDelta rowDelta = iceBergTable.newRowDelta();

                if (dataFile != null) {
                    rowDelta.addRows(dataFile);
                }
                if (deleteFile != null) {
                    rowDelta.addDeletes(deleteFile);
                }

                rowDelta.commit();

                log.info("[IceBergBatch] commit ok key={} attempt={} hasInsert={} hasDelete={}",
                        logKey, attempt, dataFile != null, deleteFile != null);
                return;

            } catch (org.apache.iceberg.exceptions.CommitFailedException cfe) {
                log.warn("[IceBergBatch] commit conflict retry key={} attempt={}/{} error={}",
                        logKey, attempt, maxRetry, cfe.getMessage());
                if (attempt >= maxRetry) {
                    throw new RuntimeException("[IceBergBatch] commit retry failed key=" + logKey, cfe);
                }
                try { Thread.sleep(200L * attempt); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }

            } catch (org.apache.iceberg.exceptions.ValidationException ve) {
                log.error("[IceBergBatch] validation failed key={} error={}", logKey, ve.getMessage());
                throw new RuntimeException("[IceBergBatch] ValidationException. key=" + logKey, ve);
            }
        }
    }

    /**
     * Resolve equality field ids: PK cols if has PK, else all cols.
     */
    private static List<Integer> resolveEqualityFieldIds(Table iceBergTable, List<String> pkNames) {
        if (pkNames != null && !pkNames.isEmpty()) {
            List<Integer> ids = pkNames.stream()
                    .map(name -> {
                        Types.NestedField field = iceBergTable.schema().findField(name);
                        if (field == null) {
                            throw new IllegalArgumentException(
                                    "[IceBergBatch] PK col not in schema: " + name);
                        }
                        return field.fieldId();
                    })
                    .collect(Collectors.toList());
            log.info("[IceBergBatch][TX] equality delete by PK table={} pkNames={} fieldIds={}",
                    iceBergTable.name(), pkNames, ids);
            return ids;
        } else {
            log.warn("[IceBergBatch] no PK, use all cols for equality delete");
            List<Integer> ids = iceBergTable.schema().columns().stream()
                    .map(Types.NestedField::fieldId)
                    .collect(Collectors.toList());
            log.info("[IceBergBatch][TX] equality delete by ALL_COLS table={} fieldIds={}",
                    iceBergTable.name(), ids);
            return ids;
        }
    }

    /**
     * Extract PK key from op. INSERT/UPDATE use newRecord, DELETE use oldRecord.
     */
    private static String extractPkKey(RowOperation op, List<String> pkNames) {
        GenericRecord record = chooseRecordForPk(op, pkNames);
        if (record == null) {
            throw new IllegalArgumentException("[IceBergBatch] op has no record: " + op);
        }

        if (pkNames != null && !pkNames.isEmpty()) {
            return pkNames.stream()
                    .map(pk -> safeString(record.getField(pk)))
                    .collect(Collectors.joining("|"));
        } else {
            return record.struct().fields().stream()
                    .map(f -> safeString(record.getField(f.name())))
                    .collect(Collectors.joining("|"));
        }
    }

    private static String safeString(Object val) {
        return val == null ? "__NULL__" : val.toString();
    }

    private static GenericRecord chooseRecordForPk(RowOperation op, List<String> pkNames) {
        GenericRecord newRecord = op.getNewRecord();
        GenericRecord oldRecord = op.getOldRecord();

        if (op.getType() == RowOperation.OpType.DELETE) {
            if (hasPkValue(oldRecord, pkNames)) {
                return oldRecord;
            }
            if (hasPkValue(newRecord, pkNames)) {
                return newRecord;
            }
            return oldRecord != null ? oldRecord : newRecord;
        }

        if (hasPkValue(newRecord, pkNames)) {
            return newRecord;
        }
        if (hasPkValue(oldRecord, pkNames)) {
            return oldRecord;
        }
        return newRecord != null ? newRecord : oldRecord;
    }

    private static boolean hasPkValue(GenericRecord record, List<String> pkNames) {
        if (record == null) {
            return false;
        }
        if (pkNames == null || pkNames.isEmpty()) {
            return true;
        }
        for (String pk : pkNames) {
            Object v = record.getField(pk);
            if (v == null || String.valueOf(v).isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private static String summarizeRecords(List<GenericRecord> records, List<String> keys, int limit) {
        if (records == null || records.isEmpty()) {
            return "[]";
        }
        List<String> out = new ArrayList<>();
        int max = Math.min(records.size(), limit);
        for (int i = 0; i < max; i++) {
            out.add(extractRecordKey(records.get(i), keys));
        }
        if (records.size() > limit) {
            out.add("...+" + (records.size() - limit));
        }
        return out.toString();
    }

    private static String extractRecordKey(GenericRecord record, List<String> keys) {
        if (record == null) {
            return "__NULL_RECORD__";
        }
        List<String> useKeys = keys;
        if (useKeys == null || useKeys.isEmpty()) {
            useKeys = record.struct().fields().stream()
                    .map(Types.NestedField::name)
                    .collect(Collectors.toList());
        }
        return useKeys.stream()
                .map(k -> k + "=" + safeString(record.getField(k)))
                .collect(Collectors.joining(","));
    }

    private static class MergeResult {
        final List<GenericRecord> insertRecords;
        final List<GenericRecord> deleteRecords;

        MergeResult(List<GenericRecord> insertRecords, List<GenericRecord> deleteRecords) {
            this.insertRecords = insertRecords;
            this.deleteRecords = deleteRecords;
        }
    }
}