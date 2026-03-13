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
     * @param immuTableKeyName e.g. db.table.threadId
     * @param tableKeyName     e.g. db.table
     * @param orderedOps       sorted by binlogOffset
     */
    public static class FlushMetrics {
        public final int dataFilesCount;
        public final int deleteFilesCount;
        public FlushMetrics(int dataFilesCount, int deleteFilesCount) {
            this.dataFilesCount = dataFilesCount;
            this.deleteFilesCount = deleteFilesCount;
        }
    }

    public static FlushMetrics flushBatch(String immuTableKeyName,
                                          String tableKeyName,
                                          List<RowOperation> allOps) throws Exception {

        if (allOps == null || allOps.isEmpty()) {
            log.info("[IceBergBatch][tid={}] empty batch, skip key={}", Thread.currentThread().getId(), immuTableKeyName);
            return new FlushMetrics(0, 0);
        }

        Table iceBergTable = GlobalSetConfInfo.IceBergCacheTableMap.get(tableKeyName);
        if (iceBergTable == null) {
            throw new IllegalStateException("[IceBergBatch] table not found: " + tableKeyName);
        }

        List<String> pkNames = GlobalSetConfInfo.TablePkColCacheMap.get(tableKeyName);

        // [Feature] Support \"Trajectory Table\" (No PK): I/D -> 1 row, U -> 2 rows
        // (Before & After)
        if (pkNames == null || pkNames.isEmpty()) {
            log.info("[IceBergBatch][tid={}][Trajectory] No PK table, writing all ops as audit records, table={} ops={}", Thread.currentThread().getId(), tableKeyName, allOps.size());

            List<GenericRecord> recordsToWrite = allOps.stream()
                    .flatMap(op -> {
                        List<GenericRecord> rows = new ArrayList<>();
                        switch (op.getType()) {
                            case INSERT:
                                if (op.getNewRecord() != null)
                                    rows.add(op.getNewRecord());
                                break;
                            case DELETE:
                                if (op.getOldRecord() != null)
                                    rows.add(op.getOldRecord());
                                break;
                            case UPDATE:
                                if (op.getOldRecord() != null)
                                    rows.add(op.getOldRecord());
                                if (op.getNewRecord() != null)
                                    rows.add(op.getNewRecord());
                                break;
                        }
                        return rows.stream();
                    })
                    .collect(Collectors.toList());

            if (recordsToWrite.isEmpty())
                return new FlushMetrics(0, 0);

            List<DataFile> dataFiles = writePartitionedDataFiles(iceBergTable, recordsToWrite, tableKeyName);

            // Even in \"transaction\" mode, No-PK tables must use Append to keep all
            // trajectory rows visible
            AppendFiles appendFiles = iceBergTable.newAppend();
            for (DataFile df : dataFiles) {
                appendFiles.appendFile(df);
            }
            appendFiles.commit();

            log.info("[IceBergBatch][tid={}][Trajectory] commit ok key={} rows={} records={}", Thread.currentThread().getId(), tableKeyName, allOps.size(), recordsToWrite.size());
            return new FlushMetrics(dataFiles.size(), 0);
        }

        if ("transaction".equals(Constant.icebergWriteMode)) {
            log.info("[IceBergBatch][tid={}][TX] flush start table={} ops={} pkNames={}", Thread.currentThread().getId(), tableKeyName, allOps.size(), pkNames);
            MergeResult mergeResult = mergeByPrimaryKey(allOps, pkNames, iceBergTable, tableKeyName);
            log.info("[IceBergBatch][tid={}][TX] merge done table={} insertCount={} deleteCount={}", Thread.currentThread().getId(), tableKeyName, mergeResult.insertRecords.size(), mergeResult.deleteRecords.size());

            // 1. Insert/Update data files
            List<DataFile> dataFiles = mergeResult.insertRecords.isEmpty() ? null
                    : writePartitionedDataFiles(iceBergTable, mergeResult.insertRecords, tableKeyName);

            List<DeleteFile> deleteFiles = null;
            if (!mergeResult.deleteRecords.isEmpty()) {
                List<Integer> equalityFieldIds = resolveEqualityFieldIds(iceBergTable, pkNames);
                deleteFiles = writeDeleteFile(iceBergTable, mergeResult.deleteRecords, equalityFieldIds, pkNames);
            }

            // 2. Commit as RowDelta (Upsert)
            commitRowDelta(iceBergTable, dataFiles, deleteFiles, tableKeyName);

            int dataFilesCount = dataFiles == null ? 0 : dataFiles.size();
            int deleteFilesCount = deleteFiles == null ? 0 : deleteFiles.size();
            log.info("[IceBergBatch][tid={}] transaction commit ok key={} inserts={} deletes={} dataFiles={} deleteFiles={}", 
                     Thread.currentThread().getId(), tableKeyName, mergeResult.insertRecords.size(), mergeResult.deleteRecords.size(), dataFilesCount, deleteFilesCount);
            return new FlushMetrics(dataFilesCount, deleteFilesCount);
        } else {
            List<GenericRecord> recordsToWrite = allOps.stream()
                    .map(op -> op.getNewRecord() != null ? op.getNewRecord() : op.getOldRecord())
                    .collect(Collectors.toList());

            List<DataFile> dataFiles = writePartitionedDataFiles(iceBergTable, recordsToWrite, tableKeyName);

            AppendFiles appendFiles = iceBergTable.newAppend();
            for (DataFile df : dataFiles) {
                appendFiles.appendFile(df);
            }
            appendFiles.commit();

            log.info("[IceBergBatch][tid={}] trajectory commit ok key={} rows={} dataFiles={}", Thread.currentThread().getId(), tableKeyName, allOps.size(), dataFiles.size());
            return new FlushMetrics(dataFiles.size(), 0);
        }
    }

    /**
     * Merge ops by PK, keep final state.
     */
    private static MergeResult mergeByPrimaryKey(List<RowOperation> orderedOps,
                                                 List<String> pkNames,
                                                 Table iceBergTable,
                                                 String tableKeyName) {
        LinkedHashMap<String, RowOperation> mergedMap = new LinkedHashMap<>();

/*        if (Constant.debugLogEnabled) {
            log.info("[IceBergBatch][tid={}] ===== mergeByPrimaryKey start table={}, ops count={} =====", Thread.currentThread().getId(), tableKeyName, orderedOps.size());
        }*/

        for (RowOperation op : orderedOps) {
            String pk = extractPkKey(op, pkNames);
            RowOperation existing = mergedMap.get(pk);

/*            if (Constant.debugLogEnabled) {
                log.info("[IceBergBatch][tid={}] table={} Processing op: type={} pk={} offset={} hasNew={} hasOld={}", Thread.currentThread().getId(), tableKeyName, op.getType(), pk, op.getBinlogOffset(), op.getNewRecord() != null, op.getOldRecord() != null);
            }*/

            if (existing == null) {
                mergedMap.put(pk, op);
/*                if (Constant.debugLogEnabled) {
                    log.info("[IceBergBatch][tid={}] table={} -> new pk={}", Thread.currentThread().getId(), tableKeyName, pk);
                }*/
            } else {
                RowOperation folded = foldOperations(existing, op, tableKeyName);
                if (folded == null) {
                    mergedMap.remove(pk);
/*                    if (Constant.debugLogEnabled) {
                        log.info("[IceBergBatch][tid={}] table={} -> cancelled pk={}", Thread.currentThread().getId(), tableKeyName, pk);
                    }*/
                } else {
                    mergedMap.put(pk, folded);
/*                    if (Constant.debugLogEnabled) {
                        log.info("[IceBergBatch][tid={}] table={} -> folded to type={} pk={}", Thread.currentThread().getId(), tableKeyName, folded.getType(), pk);
                    }*/
                }
            }
        }

/*        if (Constant.debugLogEnabled) {
            log.info("[IceBergBatch][tid={}] ===== mergeByPrimaryKey end table={}, mergedMap size={} =====", Thread.currentThread().getId(), tableKeyName, mergedMap.size());
            for (Map.Entry<String, RowOperation> entry : mergedMap.entrySet()) {
                log.info("[IceBergBatch][tid={}] table={} Final merged: pk={} -> type={}", Thread.currentThread().getId(), tableKeyName, entry.getKey(), entry.getValue().getType());
            }
        }*/

        List<GenericRecord> finalInserts = new ArrayList<>();
        List<GenericRecord> finalDeletes = new ArrayList<>();

        for (RowOperation op : mergedMap.values()) {
            switch (op.getType()) {
                case INSERT:
                    finalInserts.add(op.getNewRecord());
/*                    if (Constant.debugLogEnabled) {
                        log.info("[IceBergBatch][tid={}] table={} Final INSERT: pk={}", Thread.currentThread().getId(), tableKeyName, extractRecordKey(op.getNewRecord(), pkNames));
                    }*/
                    break;

                case DELETE:
                    if (op.getOldRecord() != null) {
                        finalDeletes.add(op.getOldRecord());
/*                        if (Constant.debugLogEnabled) {
                            log.info("[IceBergBatch][tid={}] table={} Final DELETE: pk={}", Thread.currentThread().getId(), tableKeyName, extractRecordKey(op.getOldRecord(), pkNames));
                        }*/
                    } else {
                        log.warn("[IceBergBatch][tid={}] table={} DELETE op has no record, skip. op={}", Thread.currentThread().getId(), tableKeyName, op);
                    }
                    break;

                case UPDATE:
                    if (op.getOldRecord() != null) {
                        finalDeletes.add(op.getOldRecord());
/*                        if (Constant.debugLogEnabled) {
                            log.info("[IceBergBatch][tid={}] table={} Final UPDATE del: pk={}", Thread.currentThread().getId(), tableKeyName, extractRecordKey(op.getOldRecord(), pkNames));
                        }*/
                    } else {
                        log.warn("[IceBergBatch][tid={}] table={} UPDATE op oldRecord null, insert only. op={}", Thread.currentThread().getId(), tableKeyName, op);
                    }
                    if (op.getNewRecord() != null) {
                        finalInserts.add(op.getNewRecord());
/*                        if (Constant.debugLogEnabled) {
                            log.info("[IceBergBatch][tid={}] table={} Final UPDATE ins: pk={}", Thread.currentThread().getId(), tableKeyName, extractRecordKey(op.getNewRecord(), pkNames));
                        }*/
                    }
                    break;

                default:
                    log.warn("[IceBergBatch][tid={}] table={} unknown op type: {}", Thread.currentThread().getId(), tableKeyName, op.getType());
            }
        }

        return new MergeResult(finalInserts, finalDeletes);
    }

    /**
     * Fold existing + incoming. Return null if mutually cancelled.
     */
    private static RowOperation foldOperations(RowOperation existing, RowOperation incoming, String tableKeyName) {
        RowOperation.OpType existType = existing.getType();
        RowOperation.OpType incomType = incoming.getType();
/*        if (Constant.debugLogEnabled) {
            log.info("[IceBergBatch][tid={}] table={} fold {}@{} + {}@{}", Thread.currentThread().getId(), tableKeyName, existType, existing.getBinlogOffset(), incomType, incoming.getBinlogOffset());
        }*/

        if (existType == RowOperation.OpType.INSERT && incomType == RowOperation.OpType.INSERT) {
            log.warn("[IceBergBatch][tid={}] table={} dup INSERT same PK, use latter", Thread.currentThread().getId(), tableKeyName);
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
            log.warn("[IceBergBatch][tid={}] dup DELETE same PK, use latter", Thread.currentThread().getId());
            return incoming;
        }

        if (existType == RowOperation.OpType.DELETE && incomType == RowOperation.OpType.UPDATE) {
            return RowOperation.update(existing.getOldRecord(), incoming.getNewRecord(), incoming.getBinlogOffset());
        }

        log.warn("[IceBergBatch][tid={}] unhandled fold: {} + {}", Thread.currentThread().getId(), existType, incomType);
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

    /**
     * private static DeleteFile writeDeleteFile(Table iceBergTable,
     * List<GenericRecord> deleteRecords,
     * List<Integer> equalityFieldIds) throws Exception {
     * String deletePath = iceBergTable.location() + "/delete/eq_del_" +
     * UUID.randomUUID();
     * OutputFile deleteOut = iceBergTable.io().newOutputFile(deletePath);
     *
     * EqualityDeleteWriter<GenericRecord> deleteWriter =
     * Parquet.writeDeletes(deleteOut)
     * .forTable(iceBergTable)
     * .createWriterFunc(GenericParquetWriter::buildWriter)
     * .equalityFieldIds(equalityFieldIds)
     * .buildEqualityWriter();
     * try {
     * for (GenericRecord rec : deleteRecords) {
     * deleteWriter.write(rec);
     * }
     * } finally {
     * deleteWriter.close();
     * }
     *
     * log.debug("[IceBergBatch] DeleteFile written path={} rows={}", deletePath,
     * deleteRecords.size());
     * return deleteWriter.result().deleteFiles().get(0);
     * }
     */
    private static List<DeleteFile> writeDeleteFile(Table iceBergTable,
                                                    List<GenericRecord> deleteRecords,
                                                    List<Integer> equalityFieldIds,
                                                    List<String> pkNames) throws Exception {
        if (deleteRecords == null || deleteRecords.isEmpty())
            return Collections.emptyList();

        PartitionSpec spec = iceBergTable.spec();
        boolean isPartitioned = spec.isPartitioned();

        // 构建仅含 PK 列的 schema，equality delete 不需要写全部列
        List<Types.NestedField> pkFields = new ArrayList<>();
        for (String pkName : pkNames) {
            Types.NestedField field = iceBergTable.schema().findField(pkName);
            if (field != null) {
                pkFields.add(field);
            }
        }
        Schema pkOnlySchema = new Schema(pkFields);

        // 按分区键分组
        Map<PartitionKey, List<GenericRecord>> partitionMap = new HashMap<>();
        for (GenericRecord record : deleteRecords) {
            // 跳过 PK 值为 null 的记录，无法定位要删除的行
            boolean pkValid = true;
            for (String pk : pkNames) {
                Object val = record.getField(pk);
                if (val == null || String.valueOf(val).isEmpty()) {
                    pkValid = false;
                    break;
                }
            }
            if (!pkValid) {
                log.warn("[IceBergBatch][tid={}] skip delete record with null PK, pkNames={}", Thread.currentThread().getId(), pkNames);
                continue;
            }

            PartitionKey pKey = new PartitionKey(spec, iceBergTable.schema());
            if (isPartitioned) {
                pKey.partition(record);
            }
            partitionMap.computeIfAbsent(pKey.copy(), k -> new ArrayList<>()).add(record);
        }

        if (partitionMap.isEmpty())
            return Collections.emptyList();

        List<DeleteFile> deleteFiles = new ArrayList<>();

        for (Map.Entry<PartitionKey, List<GenericRecord>> entry : partitionMap.entrySet()) {
            String deletePath = iceBergTable.location() + "/delete/eq_del_" + UUID.randomUUID() + ".parquet";
            OutputFile deleteOut = iceBergTable.io().newOutputFile(deletePath);

            Parquet.DeleteWriteBuilder builder = Parquet.writeDeletes(deleteOut)
                    .forTable(iceBergTable)
                    .rowSchema(pkOnlySchema) // 仅写 PK 列，避免非 PK 列 null 触发 NPE
                    .createWriterFunc(GenericParquetWriter::buildWriter)
                    .equalityFieldIds(equalityFieldIds);

            if (isPartitioned) {
                builder.withSpec(spec).withPartition(entry.getKey());
            }

            EqualityDeleteWriter<GenericRecord> deleteWriter = builder.buildEqualityWriter();
            try {
                for (GenericRecord rec : entry.getValue()) {
                    // 投影为仅含 PK 列的 record
                    GenericRecord pkRecord = GenericRecord.create(pkOnlySchema);
                    for (String pk : pkNames) {
                        pkRecord.setField(pk, rec.getField(pk));
                    }
                    deleteWriter.write(pkRecord);
                }
            } finally {
                deleteWriter.close();
            }
            deleteFiles.add(deleteWriter.result().deleteFiles().get(0));
        }

        return deleteFiles;
    }



    public static void flushAllThreadsForTable(String tableKeyName) throws Exception {
        List<RowOperation> allOps = new ArrayList<>();
        List<String> keysToRemove = new ArrayList<>();

        // 新增：记录每个队列被抽出来的数据，用于失败回滚
        Map<String, List<RowOperation>> backupMap = new HashMap<>();

        for (Map.Entry<String, java.util.concurrent.LinkedBlockingQueue<RowOperation>> entry : GlobalSetConfInfo.IceBergSchemaImmuTableOpsMap.entrySet()) {
            String key = entry.getKey();
            if (!key.startsWith(tableKeyName + ".")) continue;

            List<RowOperation> drained = new ArrayList<>();
            entry.getValue().drainTo(drained);
            if (!drained.isEmpty()) {
                allOps.addAll(drained);
                keysToRemove.add(key);
                backupMap.put(key, drained); // 备份数据
            }
        }

        if (allOps.isEmpty()) return;

        allOps.sort(Comparator.comparingLong(RowOperation::getBinlogOffset));
        
        String batchId = UUID.randomUUID().toString().substring(0, 8);

        try {
            log.info("[IceBergBatch][tid={}] >>> Preparing to submit batchId={} for table={} opsCount={}", 
                    Thread.currentThread().getId(), batchId, tableKeyName, allOps.size());
            
            // 尝试写入 Iceberg
            FlushMetrics metrics = flushBatch(tableKeyName, tableKeyName, allOps);

            // 写入成功后再清理缓存
            keysToRemove.forEach(k -> {
                GlobalSetConfInfo.IceBergTableGnericCacheMap.remove(k);
                GlobalConfInfo.lastDataWriteTimerByParquetMap.remove(k);
                GlobalConfInfo.engineAtomicByTableKeyMap.remove(k);
            });
            
            log.info("[IceBergBatch][tid={}] <<< Successfully submitted batchId={} for table={} dataFiles={} deleteFiles={}", 
                    Thread.currentThread().getId(), batchId, tableKeyName, metrics.dataFilesCount, metrics.deleteFilesCount);

        } catch (Exception e) {
            // 写入失败：将备份数据回滚到各自的队列中
            log.error("[IceBergBatch][tid={}] !!! Flush failed for batchId={}, table={}, rolling back {} ops to queue.", 
                    Thread.currentThread().getId(), batchId, tableKeyName, allOps.size(), e);
            for (Map.Entry<String, List<RowOperation>> backupEntry : backupMap.entrySet()) {
                java.util.concurrent.LinkedBlockingQueue<RowOperation> queue = GlobalSetConfInfo.IceBergSchemaImmuTableOpsMap.get(backupEntry.getKey());
                if (queue != null) {
                    queue.addAll(backupEntry.getValue());
                }
            }
            // 抛出异常让外层 Timer 知道本次执行失败
            throw e;
        }
    }

    private static void commitRowDelta(Table iceBergTable,
                                       List<DataFile> dataFiles,
                                       List<DeleteFile> deleteFiles,
                                       String logKey) {
        int maxRetry = 3;
        int attempt = 0;

        while (attempt < maxRetry) {
            attempt++;
            try {
                iceBergTable.refresh();
                RowDelta rowDelta = iceBergTable.newRowDelta();

                if (dataFiles != null) {
                    for (DataFile df : dataFiles) {
                        rowDelta.addRows(df);
                    }
                }
                if (deleteFiles != null) {
                    for (DeleteFile df : deleteFiles) {
                        rowDelta.addDeletes(df);
                    }
                }

                rowDelta.commit();
                
                int dataFileCount = dataFiles == null ? 0 : dataFiles.size();
                int deleteFileCount = deleteFiles == null ? 0 : deleteFiles.size();
                log.info("[IceBergBatch][tid={}] commit ok key={} attempt={} dataFiles={} deleteFiles={}", 
                         Thread.currentThread().getId(), logKey, attempt, dataFileCount, deleteFileCount);
                return;

            } catch (org.apache.iceberg.exceptions.CommitFailedException cfe) {
                log.warn("[IceBergBatch][tid={}] commit conflict retry key={} attempt={}/{} error={}", Thread.currentThread().getId(), logKey, attempt, maxRetry, cfe.getMessage());
                if (attempt >= maxRetry)
                    throw new RuntimeException("[IceBergBatch] commit retry failed key=" + logKey, cfe);
                try {
                    Thread.sleep(200L * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            } catch (org.apache.iceberg.exceptions.ValidationException ve) {
                log.error("[IceBergBatch][tid={}] validation failed key={} error={}", Thread.currentThread().getId(), logKey, ve.getMessage());
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
            log.info("[IceBergBatch][tid={}][TX] equality delete by PK table={} pkNames={} fieldIds={}", Thread.currentThread().getId(), iceBergTable.name(), pkNames, ids);
            return ids;
        } else {
            log.warn("[IceBergBatch][tid={}] no PK, use all cols for equality delete", Thread.currentThread().getId());
            List<Integer> ids = iceBergTable.schema().columns().stream()
                    .map(Types.NestedField::fieldId)
                    .collect(Collectors.toList());
            log.info("[IceBergBatch][tid={}][TX] equality delete by ALL_COLS table={} fieldIds={}", Thread.currentThread().getId(), iceBergTable.name(), ids);
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

    public static List<DataFile> writePartitionedDataFiles(Table table, List<GenericRecord> records,
                                                           String tableKeyName) throws Exception {
        if (records == null || records.isEmpty())
            return Collections.emptyList();

        PartitionSpec spec = table.spec();
        boolean isPartitioned = spec.isPartitioned();
        // 鏍稿績锛氭寜鍒嗗尯閿璁板綍杩涜鍒嗙粍
        Map<PartitionKey, List<GenericRecord>> partitionMap = new HashMap<>();
        for (GenericRecord record : records) {
            PartitionKey pKey = new PartitionKey(spec, table.schema());
            if (isPartitioned) {
                pKey.partition(record);
            }
            partitionMap.computeIfAbsent(pKey.copy(), k -> new ArrayList<>()).add(record);
        }

        List<DataFile> dataFiles = new ArrayList<>();
        for (Map.Entry<PartitionKey, List<GenericRecord>> entry : partitionMap.entrySet()) {
            OutputFile outputFile = table.io().newOutputFile(
                    new Path(table.location(), "data/" + tableKeyName.replace(".", "/")
                            + "/" + UUID.randomUUID() + ".parquet").toString());

            // 娉ㄦ剰锛氳繖閲屼篃浣跨敤浜?new Schema() 鏉ヤ繚璇佸垪鏁扮粷瀵瑰榻愶紝閬垮厤浣犱箣鍓嶉亣鍒扮殑
            // ArrayIndexOutOfBoundsException
            FileAppender<GenericRecord> appender = Parquet.write(outputFile)
                    .schema(new Schema(entry.getValue().get(0).struct().fields()))
                    .createWriterFunc(GenericParquetWriter::buildWriter)
                    .build();

            try {
                appender.addAll(entry.getValue());
            } finally {
                // 鏍稿績淇锛氬繀椤诲厛褰诲簳鍏抽棴鍐欏叆娴侊紝HDFS 涓婄殑鏂囦欢鎵嶇湡姝ｅ彲瑙?
                appender.close();
            }

            // 鍏抽棴娴佷箣鍚庯紝鍐嶅幓璇诲彇鏂囦欢鐘舵€佸拰 metrics
            DataFiles.Builder builder = DataFiles.builder(spec)
                    .withInputFile(outputFile.toInputFile())
                    .withMetrics(appender.metrics())
                    .withFormat(FileFormat.PARQUET);

            if (isPartitioned) {
                builder.withPartition(entry.getKey());
            }
            dataFiles.add(builder.build());
        }
        return dataFiles;
    }
}