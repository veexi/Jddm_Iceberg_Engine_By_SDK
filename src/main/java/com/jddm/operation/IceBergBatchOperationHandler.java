package com.jddm.operation;

import com.jddm.common.Constant;
import com.jddm.conf.GlobalConfInfo;
import com.jddm.conf.GlobalSetConfInfo;
import com.jddm.vo.RowOperation;
import com.jddm.operation.timer.IcebergFullLoadAsyncCommitter;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.*;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.File;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;

/**
 * Batch handler: merge ops by PK, write data/delete files, commit RowDelta or COW.
 */
public class IceBergBatchOperationHandler {

    private static final Logger log = LogManager.getLogger(IceBergBatchOperationHandler.class);

    private static final Map<String, java.util.concurrent.locks.ReentrantLock> tableFlushLocks =
            new java.util.concurrent.ConcurrentHashMap<>();

    public static class FlushMetrics {
        public final int dataFilesCount;
        public final int deleteFilesCount;
        public final java.util.List<String> dataFilePaths;
        public final java.util.List<String> deleteFilePaths;

        public FlushMetrics(int dataFilesCount, int deleteFilesCount,
                            java.util.List<String> dataFilePaths,
                            java.util.List<String> deleteFilePaths) {
            this.dataFilesCount = dataFilesCount;
            this.deleteFilesCount = deleteFilesCount;
            this.dataFilePaths = dataFilePaths;
            this.deleteFilePaths = deleteFilePaths;
        }
    }

    public static FlushMetrics flushBatch(String immuTableKeyName,
                                          String tableKeyName,
                                          List<RowOperation> allOps) throws Exception {

        if (allOps == null || allOps.isEmpty()) {
            log.info("[IceBergBatch][tid={}] empty batch, skip key={}",
                    Thread.currentThread().getId(), immuTableKeyName);
            return new FlushMetrics(0, 0, Collections.emptyList(), Collections.emptyList());
        }

        Table iceBergTable = GlobalSetConfInfo.IceBergCacheTableMap.get(tableKeyName);
        if (iceBergTable == null) {
            throw new IllegalStateException("[IceBergBatch] table not found: " + tableKeyName);
        }

        List<String> pkNames = GlobalSetConfInfo.TablePkColCacheMap.get(tableKeyName);

        // No-PK table: trajectory mode, append all ops directly
        if (pkNames == null || pkNames.isEmpty()) {
            log.info("[IceBergBatch][tid={}][Trajectory] No PK table, writing all ops as audit records, table={} ops={}",
                    Thread.currentThread().getId(), tableKeyName, allOps.size());

            List<GenericRecord> recordsToWrite = allOps.stream()
                    .flatMap(op -> {
                        List<GenericRecord> rows = new ArrayList<>();
                        switch (op.getType()) {
                            case INSERT:
                                if (op.getNewRecord() != null) rows.add(op.getNewRecord());
                                break;
                            case DELETE:
                                if (op.getOldRecord() != null) rows.add(op.getOldRecord());
                                break;
                            case UPDATE:
                                if (op.getOldRecord() != null) rows.add(op.getOldRecord());
                                if (op.getNewRecord() != null) rows.add(op.getNewRecord());
                                break;
                        }
                        return rows.stream();
                    })
                    .collect(Collectors.toList());

            if (recordsToWrite.isEmpty())
                return new FlushMetrics(0, 0, Collections.emptyList(), Collections.emptyList());

            List<DataFile> dataFiles = writePartitionedDataFiles(iceBergTable, recordsToWrite, tableKeyName);
            List<String> dataPaths = dataFiles.stream().map(df -> df.path().toString()).collect(Collectors.toList());

            AppendFiles appendFiles = iceBergTable.newAppend();
            for (DataFile df : dataFiles) appendFiles.appendFile(df);
            appendFiles.commit();

            log.info("[IceBergBatch][tid={}][Trajectory] commit ok key={} rows={} records={} dataFiles={}",
                    Thread.currentThread().getId(), tableKeyName, allOps.size(), recordsToWrite.size(), dataPaths);
            return new FlushMetrics(dataFiles.size(), 0, dataPaths, Collections.emptyList());
        }

        // Full-load fast path: skip mergeByPrimaryKey and delete file generation.
        // This branch is only reached when flushBatch is called directly (e.g. in tests).
        // Normal full-load traffic goes through writeFullLoadStreamFromQueue in flushAllThreadsForTable.
        boolean isFullLoad = allOps.get(0).isFullLoad();
        if (isFullLoad) {
            int totalOps = allOps.size();
            log.info("[IceBergBatch][tid={}][FullLoad] detected full load batch, writing to local cache. table={} ops={}",
                    Thread.currentThread().getId(), tableKeyName, totalOps);

            if (totalOps == 0) {
                return new FlushMetrics(0, 0, Collections.emptyList(), Collections.emptyList());
            }

            long writeStart = System.currentTimeMillis();
            List<DataFile> dataFiles = writePartitionedDataFilesToLocal(iceBergTable, allOps, tableKeyName);
            allOps.clear();
            long writeEnd = System.currentTimeMillis();

            List<String> dataPaths = dataFiles.stream()
                    .map(df -> "[LOCAL]" + df.path().toString()).collect(Collectors.toList());

            long cacheStart = System.currentTimeMillis();
            for (DataFile df : dataFiles) {
                File localFile = new File(df.path().toString());
                IcebergFullLoadAsyncCommitter.saveToLocalCache(tableKeyName, df, localFile);
            }
            long cacheEnd = System.currentTimeMillis();

            log.info("[IceBergBatch][tid={}][FullLoad] local cache ok table={} rows={} writeMs={} cacheMs={} totalMs={} dataFiles={}",
                    Thread.currentThread().getId(), tableKeyName, totalOps,
                    writeEnd - writeStart, cacheEnd - cacheStart, cacheEnd - writeStart, dataPaths);
            return new FlushMetrics(dataFiles.size(), 0, dataPaths, Collections.emptyList());
        }

        if ("transaction".equals(Constant.icebergWriteMode)) {

            // ============================================================
            // COW batch mode: scan existing files and rewrite to deduplicate.
            // Suitable for Hive 3.x which does not support equality delete.
            // ============================================================
            if ("batch".equals(Constant.cowMode)) {
                log.info("[IceBergBatch][tid={}][COW] flush start table={} ops={} pkNames={}",
                        Thread.currentThread().getId(), tableKeyName, allOps.size(), pkNames);

                MergeResult mergeResult = mergeByPrimaryKey(allOps, pkNames, iceBergTable, tableKeyName);
                log.info("[IceBergBatch][tid={}][COW] merge done table={} insertCount={} deleteCount={}",
                        Thread.currentThread().getId(), tableKeyName,
                        mergeResult.insertRecords.size(), mergeResult.deleteRecords.size());

                // 1. Collect all affected PKs from both insert and delete sides
                Set<String> affectedPks = new HashSet<>();
                for (GenericRecord r : mergeResult.insertRecords) {
                    affectedPks.add(buildPkString(r, pkNames));
                }
                for (GenericRecord r : mergeResult.deleteRecords) {
                    affectedPks.add(buildPkString(r, pkNames));
                }

                // 2. Scan existing Iceberg data, filter out affected PKs
                List<GenericRecord> survivingRecords = new ArrayList<>();
                List<DataFile> oldDataFiles = new ArrayList<>();

                if (!affectedPks.isEmpty()) {
                    iceBergTable.refresh();
                    try (CloseableIterable<FileScanTask> tasks = iceBergTable.newScan().planFiles()) {
                        for (FileScanTask task : tasks) {
                            List<GenericRecord> fileRecords = readDataFile(task, iceBergTable);
                            List<GenericRecord> kept = new ArrayList<>();
                            boolean hasMatch = false;
                            for (GenericRecord rec : fileRecords) {
                                String pk = buildPkString(rec, pkNames);
                                if (affectedPks.contains(pk)) {
                                    hasMatch = true;
                                } else {
                                    kept.add(rec);
                                }
                            }
                            if (hasMatch) {
                                survivingRecords.addAll(kept);
                                oldDataFiles.add(task.file());
                            }
                        }
                    }
                }

                // 3. Merge: surviving old records + new inserts from this batch
                survivingRecords.addAll(mergeResult.insertRecords);

                log.info("[IceBergBatch][tid={}][COW] table={} oldDataFiles={} survivingRecords={} newInserts={}",
                        Thread.currentThread().getId(), tableKeyName,
                        oldDataFiles.size(), survivingRecords.size() - mergeResult.insertRecords.size(),
                        mergeResult.insertRecords.size());

                // 4. No old files hit: directly append new data
                if (oldDataFiles.isEmpty()) {
                    if (mergeResult.insertRecords.isEmpty()) {
                        log.info("[IceBergBatch][tid={}][COW] nothing to write table={}",
                                Thread.currentThread().getId(), tableKeyName);
                        return new FlushMetrics(0, 0, Collections.emptyList(), Collections.emptyList());
                    }
                    List<DataFile> newDataFiles = writePartitionedDataFiles(
                            iceBergTable, mergeResult.insertRecords, tableKeyName);
                    List<String> dataPaths = newDataFiles.stream()
                            .map(df -> df.path().toString()).collect(Collectors.toList());
                    AppendFiles appendFiles = iceBergTable.newAppend();
                    for (DataFile df : newDataFiles) appendFiles.appendFile(df);
                    appendFiles.commit();
                    log.info("[IceBergBatch][tid={}][COW] append commit ok table={} dataFiles={}",
                            Thread.currentThread().getId(), tableKeyName, dataPaths);
                    return new FlushMetrics(newDataFiles.size(), 0, dataPaths, Collections.emptyList());
                }

                // 5. Write new merged data file
                List<DataFile> newDataFiles = survivingRecords.isEmpty()
                        ? Collections.emptyList()
                        : writePartitionedDataFiles(iceBergTable, survivingRecords, tableKeyName);
                List<String> dataPaths = newDataFiles.stream()
                        .map(df -> df.path().toString()).collect(Collectors.toList());

                // 6. Atomic OverwriteFiles commit: remove old files, add new files, no delete file
                int maxRetry = 3;
                for (int attempt = 1; attempt <= maxRetry; attempt++) {
                    try {
                        iceBergTable.refresh();
                        OverwriteFiles overwrite = iceBergTable.newOverwrite();
                        for (DataFile old : oldDataFiles) overwrite.deleteFile(old);
                        for (DataFile nf : newDataFiles) overwrite.addFile(nf);
                        overwrite.commit();
                        break;
                    } catch (org.apache.iceberg.exceptions.CommitFailedException cfe) {
                        log.warn("[IceBergBatch][tid={}][COW] commit conflict retry table={} attempt={}/{}",
                                Thread.currentThread().getId(), tableKeyName, attempt, maxRetry);
                        if (attempt >= maxRetry)
                            throw new RuntimeException("[COW] commit retry failed table=" + tableKeyName, cfe);
                        Thread.sleep(200L * attempt);
                    }
                }

                log.info("[IceBergBatch][tid={}][COW] overwrite commit ok table={} removedFiles={} dataFiles={}",
                        Thread.currentThread().getId(), tableKeyName, oldDataFiles.size(), dataPaths);
                return new FlushMetrics(newDataFiles.size(), 0, dataPaths, Collections.emptyList());

            } else {
                // ============================================================
                // RowDelta mode (timer / none): write equality delete file
                // ============================================================
                log.info("[IceBergBatch][tid={}][RowDelta] flush start table={} ops={} pkNames={} cowMode={}",
                        Thread.currentThread().getId(), tableKeyName, allOps.size(), pkNames, Constant.cowMode);

                MergeResult mergeResult = mergeByPrimaryKey(allOps, pkNames, iceBergTable, tableKeyName);
                log.info("[IceBergBatch][tid={}][RowDelta] merge done table={} insertCount={} deleteCount={}",
                        Thread.currentThread().getId(), tableKeyName,
                        mergeResult.insertRecords.size(), mergeResult.deleteRecords.size());

                List<DataFile> dataFiles = mergeResult.insertRecords.isEmpty() ? null
                        : writePartitionedDataFiles(iceBergTable, mergeResult.insertRecords, tableKeyName);

                List<DeleteFile> deleteFiles = null;
                if (!mergeResult.deleteRecords.isEmpty()) {
                    List<Integer> equalityFieldIds = resolveEqualityFieldIds(iceBergTable, pkNames);
                    deleteFiles = writeDeleteFile(iceBergTable, mergeResult.deleteRecords, equalityFieldIds, pkNames);
                }

                List<String> dataPaths = dataFiles == null ? Collections.emptyList()
                        : dataFiles.stream().map(df -> df.path().toString()).collect(Collectors.toList());
                List<String> deletePaths = deleteFiles == null ? Collections.emptyList()
                        : deleteFiles.stream().map(df -> df.path().toString()).collect(Collectors.toList());

                commitRowDelta(iceBergTable, dataFiles, deleteFiles, tableKeyName);

                int dataFilesCount = dataFiles == null ? 0 : dataFiles.size();
                int deleteFilesCount = deleteFiles == null ? 0 : deleteFiles.size();
                log.info("[IceBergBatch][tid={}][RowDelta] commit ok table={} inserts={} deletes={} dataFiles={} deleteFiles={}",
                        Thread.currentThread().getId(), tableKeyName,
                        mergeResult.insertRecords.size(), mergeResult.deleteRecords.size(),
                        dataPaths, deletePaths);
                return new FlushMetrics(dataFilesCount, deleteFilesCount, dataPaths, deletePaths);
            }

        } else {
            // Trajectory mode: pure Append
            List<GenericRecord> recordsToWrite = allOps.stream()
                    .map(op -> op.getNewRecord() != null ? op.getNewRecord() : op.getOldRecord())
                    .collect(Collectors.toList());

            List<DataFile> dataFiles = writePartitionedDataFiles(iceBergTable, recordsToWrite, tableKeyName);
            List<String> dataPaths = dataFiles.stream().map(df -> df.path().toString()).collect(Collectors.toList());

            AppendFiles appendFiles = iceBergTable.newAppend();
            for (DataFile df : dataFiles) appendFiles.appendFile(df);
            appendFiles.commit();

            log.info("[IceBergBatch][tid={}] trajectory commit ok key={} rows={} dataFilesCount={} dataFiles={}",
                    Thread.currentThread().getId(), tableKeyName, allOps.size(), dataFiles.size(), dataPaths);
            return new FlushMetrics(dataFiles.size(), 0, dataPaths, Collections.emptyList());
        }
    }

    // ========== COW helpers ==========

    private static final ThreadLocal<StringBuilder> PK_SB_CACHE = ThreadLocal.withInitial(() -> new StringBuilder(512));

    public static String buildPkString(GenericRecord r, List<String> pkNames) {
        StringBuilder sb = PK_SB_CACHE.get();
        sb.setLength(0);
        for (int i = 0; i < pkNames.size(); i++) {
            if (i > 0) sb.append("|");
            Object val = r.getField(pkNames.get(i));
            sb.append(val == null ? "__NULL__" : val.toString());
        }
        return sb.toString();
    }

    public static List<GenericRecord> readDataFile(FileScanTask task, Table table) throws Exception {
        List<GenericRecord> result = new ArrayList<>();
        org.apache.iceberg.io.InputFile inputFile = table.io().newInputFile(task.file().path().toString());
        try (CloseableIterable<GenericRecord> reader =
                     org.apache.iceberg.parquet.Parquet.read(inputFile)
                             .project(table.schema())
                             .createReaderFunc(fileSchema ->
                                     org.apache.iceberg.data.parquet.GenericParquetReaders.buildReader(
                                             table.schema(), fileSchema, Collections.emptyMap()))
                             .build()) {
            for (GenericRecord r : reader) {
                result.add(r.copy());
            }
        }
        return result;
    }

    // ========== PK merge ==========

    private static MergeResult mergeByPrimaryKey(List<RowOperation> orderedOps,
                                                 List<String> pkNames,
                                                 Table iceBergTable,
                                                 String tableKeyName) {
        LinkedHashMap<String, RowOperation> mergedMap = new LinkedHashMap<>();

        for (RowOperation op : orderedOps) {
            String pk = extractPkKey(op, pkNames);
            RowOperation existing = mergedMap.get(pk);
            if (existing == null) {
                mergedMap.put(pk, op);
            } else {
                RowOperation folded = foldOperations(existing, op, tableKeyName);
                if (folded == null) {
                    mergedMap.remove(pk);
                } else {
                    mergedMap.put(pk, folded);
                }
            }
        }

        List<GenericRecord> finalInserts = new ArrayList<>();
        List<GenericRecord> finalDeletes = new ArrayList<>();

        for (RowOperation op : mergedMap.values()) {
            switch (op.getType()) {
                case INSERT:
                    finalInserts.add(op.getNewRecord());
                    finalDeletes.add(op.getNewRecord()); // blind upsert: evict existing row by PK
                    break;

                case DELETE:
                    if (op.getOldRecord() != null) {
                        finalDeletes.add(op.getOldRecord());
                    } else {
                        log.warn("[IceBergBatch][tid={}] table={} DELETE op has no record, skip. op={}",
                                Thread.currentThread().getId(), tableKeyName, op);
                    }
                    break;

                case UPDATE:
                    if (op.getOldRecord() != null) {
                        finalDeletes.add(op.getOldRecord());
                    } else {
                        log.warn("[IceBergBatch][tid={}] table={} UPDATE op oldRecord null, insert only. op={}",
                                Thread.currentThread().getId(), tableKeyName, op);
                    }
                    if (op.getNewRecord() != null) {
                        finalInserts.add(op.getNewRecord());
                    }
                    break;

                default:
                    log.warn("[IceBergBatch][tid={}] table={} unknown op type: {}",
                            Thread.currentThread().getId(), tableKeyName, op.getType());
            }
        }

        return new MergeResult(finalInserts, finalDeletes);
    }

    private static RowOperation foldOperations(RowOperation existing, RowOperation incoming, String tableKeyName) {
        RowOperation.OpType existType = existing.getType();
        RowOperation.OpType incomType = incoming.getType();

        if (existType == RowOperation.OpType.INSERT && incomType == RowOperation.OpType.INSERT) {
            log.warn("[IceBergBatch][tid={}] table={} dup INSERT same PK, use latter",
                    Thread.currentThread().getId(), tableKeyName);
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

    // ========== File write helpers ==========

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
            try {
                for (GenericRecord record : records) {
                    appender.add(record);
                }
            } catch (Exception e) {
                log.error("[IceBergBatch][tid={}] Error writing data file (simple): {}, tableKeyName={}",
                        Thread.currentThread().getId(), outputFile.location(), tableKeyName, e);
                throw e;
            }
        }

        return DataFiles.builder(table.spec())
                .withInputFile(outputFile.toInputFile())
                .withMetrics(appender.metrics())
                .withFormat(format)
                .build();
    }

    private static List<DeleteFile> writeDeleteFile(Table iceBergTable,
                                                    List<GenericRecord> deleteRecords,
                                                    List<Integer> equalityFieldIds,
                                                    List<String> pkNames) throws Exception {
        if (deleteRecords == null || deleteRecords.isEmpty())
            return Collections.emptyList();

        PartitionSpec spec = iceBergTable.spec();
        boolean isPartitioned = spec.isPartitioned();

        List<Types.NestedField> pkFields = new ArrayList<>();
        for (String pkName : pkNames) {
            Types.NestedField field = iceBergTable.schema().findField(pkName);
            if (field != null) pkFields.add(field);
        }
        Schema pkOnlySchema = new Schema(pkFields);

        Map<PartitionKey, List<GenericRecord>> partitionMap = new HashMap<>();
        PartitionKey pKey = new PartitionKey(spec, iceBergTable.schema());
        for (GenericRecord record : deleteRecords) {
            boolean pkValid = true;
            for (String pk : pkNames) {
                Object val = record.getField(pk);
                if (val == null || String.valueOf(val).isEmpty()) {
                    pkValid = false;
                    break;
                }
            }
            if (!pkValid) {
                log.warn("[IceBergBatch][tid={}] skip delete record with null PK, pkNames={}",
                        Thread.currentThread().getId(), pkNames);
                continue;
            }
            if (isPartitioned) pKey.partition(record);
            partitionMap.computeIfAbsent(pKey.copy(), k -> new ArrayList<>()).add(record);
        }

        if (partitionMap.isEmpty()) return Collections.emptyList();

        List<DeleteFile> deleteFiles = new ArrayList<>();
        for (Map.Entry<PartitionKey, List<GenericRecord>> entry : partitionMap.entrySet()) {
            String deletePath = iceBergTable.location() + "/delete/eq_del_" + UUID.randomUUID() + ".parquet";
            OutputFile deleteOut;
            try {
                deleteOut = iceBergTable.io().newOutputFile(deletePath);
            } catch (Exception e) {
                log.error("[IceBergBatch][tid={}] Error creating delete file: {}, table={}",
                        Thread.currentThread().getId(), deletePath, iceBergTable.name(), e);
                throw e;
            }

            Parquet.DeleteWriteBuilder builder = Parquet.writeDeletes(deleteOut)
                    .forTable(iceBergTable)
                    .rowSchema(pkOnlySchema)
                    .createWriterFunc(GenericParquetWriter::buildWriter)
                    .equalityFieldIds(equalityFieldIds);

            if (isPartitioned) builder.withSpec(spec).withPartition(entry.getKey());

            EqualityDeleteWriter<GenericRecord> deleteWriter = builder.buildEqualityWriter();
            try {
                for (GenericRecord rec : entry.getValue()) {
                    GenericRecord pkRecord = GenericRecord.create(pkOnlySchema);
                    for (String pk : pkNames) pkRecord.setField(pk, rec.getField(pk));
                    deleteWriter.write(pkRecord);
                }
            } catch (Exception e) {
                log.error("[IceBergBatch][tid={}] Error writing delete file: {}, table={}",
                        Thread.currentThread().getId(), deletePath, iceBergTable.name(), e);
                throw e;
            } finally {
                deleteWriter.close();
            }
            deleteFiles.add(deleteWriter.result().deleteFiles().get(0));
        }
        return deleteFiles;
    }

    public static List<DataFile> writePartitionedDataFiles(Table table,
                                                           List<GenericRecord> records,
                                                           String tableKeyName) throws Exception {
        if (records == null || records.isEmpty()) return Collections.emptyList();

        PartitionSpec spec = table.spec();
        boolean isPartitioned = spec.isPartitioned();

        Map<PartitionKey, List<GenericRecord>> partitionMap = new HashMap<>();
        PartitionKey reusablePKey = new PartitionKey(spec, table.schema());

        for (GenericRecord record : records) {
            if (isPartitioned) reusablePKey.partition(record);
            partitionMap.computeIfAbsent(reusablePKey.copy(), k -> new ArrayList<>()).add(record);
        }

        List<DataFile> dataFiles = new ArrayList<>();
        for (Map.Entry<PartitionKey, List<GenericRecord>> entry : partitionMap.entrySet()) {
            StringBuilder pathSb = new StringBuilder(table.location());
            pathSb.append("/data/").append(tableKeyName.replace(".", "/")).append("/").append(UUID.randomUUID()).append(".parquet");
            String pathStr = pathSb.toString();
            OutputFile outputFile;
            try {
                outputFile = table.io().newOutputFile(pathStr);
            } catch (Exception e) {
                log.error("[IceBergBatch][tid={}] Error creating data file: {}, tableKeyName={}",
                        Thread.currentThread().getId(), pathStr, tableKeyName, e);
                throw e;
            }

            FileAppender<GenericRecord> appender = Parquet.write(outputFile)
                    .schema(table.schema())
                    .createWriterFunc(GenericParquetWriter::buildWriter)
                    .build();

            try {
                for (GenericRecord row : entry.getValue()) {
                    appender.add(row);
                }
            } catch (Exception e) {
                log.error("[IceBergBatch][tid={}] Error writing data file: {}, tableKeyName={}",
                        Thread.currentThread().getId(), outputFile.location(), tableKeyName, e);
                throw e;
            } finally {
                appender.close();
            }

            DataFiles.Builder builder = DataFiles.builder(spec)
                    .withInputFile(outputFile.toInputFile())
                    .withMetrics(appender.metrics())
                    .withFormat(FileFormat.PARQUET);

            if (isPartitioned) builder.withPartition(entry.getKey());
            dataFiles.add(builder.build());
        }
        return dataFiles;
    }

    private static class WriterContext {
        File localFile;
        org.apache.iceberg.io.OutputFile outputFile;
        FileAppender<GenericRecord> appender;

        WriterContext(File localFile, org.apache.iceberg.io.OutputFile outputFile, FileAppender<GenericRecord> appender) {
            this.localFile = localFile;
            this.outputFile = outputFile;
            this.appender = appender;
        }
    }

    public static List<DataFile> writePartitionedDataFilesToLocal(Table table,
                                                                  List<RowOperation> allOps,
                                                                  String tableKeyName) throws Exception {
        if (allOps == null || allOps.isEmpty()) return Collections.emptyList();

        PartitionSpec spec = table.spec();
        boolean isPartitioned = spec.isPartitioned();
        File cacheDir = new File(Constant.basicWorkPath, "FileCache/" + tableKeyName);
        if (!cacheDir.exists()) cacheDir.mkdirs();

        // Adaptive Parquet tuning for wide tables
        int colCount = table.schema().columns().size();
        String rowGroupSize = "134217728"; // default 128MB
        String pageSize = "1048576";       // default 1MB
        if (colCount > 100) {
            log.info("[IceBergBatch][tid={}] Wide table detected (cols={}), tuning Parquet memory for safety.",
                    Thread.currentThread().getId(), colCount);
            rowGroupSize = "33554432"; // 32MB for wide tables
            pageSize = "524288";       // 512KB
        }

        List<DataFile> resultDataFiles = new ArrayList<>();

        if (!isPartitioned) {
            // Non-partitioned: single writer, stream directly
            File localFile = new File(cacheDir, UUID.randomUUID() + ".parquet");
            org.apache.iceberg.io.OutputFile outputFile = org.apache.iceberg.Files.localOutput(localFile);

            FileAppender<GenericRecord> appender = Parquet.write(outputFile)
                    .schema(table.schema())
                    .createWriterFunc(GenericParquetWriter::buildWriter)
                    .set("write.parquet.compression-codec", "snappy")
                    .set("write.parquet.row-group-size-bytes", rowGroupSize)
                    .set("write.parquet.page-size-bytes", pageSize)
                    .build();

            try {
                for (int i = 0; i < allOps.size(); i++) {
                    GenericRecord rec = allOps.get(i).getNewRecord();
                    if (rec != null) {
                        appender.add(rec);
                    }
                    // Null out the reference immediately after write so GC can reclaim it
                    allOps.set(i, null);
                }
            } finally {
                appender.close();
            }

            DataFile df = DataFiles.builder(spec)
                    .withInputFile(outputFile.toInputFile())
                    .withMetrics(appender.metrics())
                    .withFormat(FileFormat.PARQUET)
                    .build();
            resultDataFiles.add(df);

        } else {
            // Partitioned: maintain one writer per partition, stream row by row
            Map<PartitionKey, WriterContext> writersMap = new HashMap<>();
            PartitionKey reusablePKey = new PartitionKey(spec, table.schema());

            try {
                for (int i = 0; i < allOps.size(); i++) {
                    RowOperation op = allOps.get(i);
                    GenericRecord rec = op.getNewRecord();
                    if (rec == null) {
                        allOps.set(i, null);
                        continue;
                    }

                    reusablePKey.partition(rec);
                    PartitionKey currentKey = reusablePKey.copy();

                    WriterContext context = writersMap.get(currentKey);
                    if (context == null) {
                        File localFile = new File(cacheDir, UUID.randomUUID() + ".parquet");
                        org.apache.iceberg.io.OutputFile outputFile = org.apache.iceberg.Files.localOutput(localFile);
                        FileAppender<GenericRecord> appender = Parquet.write(outputFile)
                                .schema(table.schema())
                                .createWriterFunc(GenericParquetWriter::buildWriter)
                                .set("write.parquet.compression-codec", "snappy")
                                .set("write.parquet.row-group-size-bytes", rowGroupSize)
                                .set("write.parquet.page-size-bytes", pageSize)
                                .build();
                        context = new WriterContext(localFile, outputFile, appender);
                        writersMap.put(currentKey, context);
                    }

                    context.appender.add(rec);
                    // Null out immediately so GC can reclaim the record
                    allOps.set(i, null);
                }
            } finally {
                for (Map.Entry<PartitionKey, WriterContext> entry : writersMap.entrySet()) {
                    PartitionKey pKey = entry.getKey();
                    WriterContext ctx = entry.getValue();
                    try {
                        ctx.appender.close();
                        DataFile df = DataFiles.builder(spec)
                                .withInputFile(ctx.outputFile.toInputFile())
                                .withMetrics(ctx.appender.metrics())
                                .withFormat(FileFormat.PARQUET)
                                .withPartition(pKey)
                                .build();
                        resultDataFiles.add(df);
                    } catch (Exception e) {
                        log.error("[IceBergBatch] Failed to close writer for partition: {}", pKey, e);
                    }
                }
                writersMap.clear();
            }
        }

        allOps.clear();
        return resultDataFiles;
    }

    // ========== flushAllThreadsForTable ==========

    public static void flushAllThreadsForTable(String tableKeyName) throws Exception {
        java.util.concurrent.locks.ReentrantLock tableLock =
                tableFlushLocks.computeIfAbsent(tableKeyName,
                        k -> new java.util.concurrent.locks.ReentrantLock());
        tableLock.lock();
        try {
            // ★ 改动：在冲刷增量队列前，先检查并关闭本表所有的全量直写 Appender
            flushDirectWriters(tableKeyName);

            List<String> sortedKeys = GlobalSetConfInfo.IceBergSchemaImmuTableOpsMap.keySet().stream()
                    .filter(k -> k.startsWith(tableKeyName + "."))
                    .sorted()
                    .collect(Collectors.toList());

            // ---------------------------------------------------------------
            // Probe the first element of the first non-empty queue to detect
            // full-load vs incremental mode. O(1), does not consume the queue.
            // Full-load path: queues are consumed directly by writeFullLoadStreamFromQueue,
            //   so we must NOT drainTo allOps — doing so would double memory usage.
            // Incremental path: drainTo allOps as before for cross-thread sort + PK merge.
            // ---------------------------------------------------------------
            boolean isAllFullLoad = false;
            for (String key : sortedKeys) {
                LinkedBlockingDeque<RowOperation> queue =
                        GlobalSetConfInfo.IceBergSchemaImmuTableOpsMap.get(key);
                if (queue == null || queue.isEmpty()) continue;
                RowOperation first = queue.peekFirst();
                if (first != null) {
                    isAllFullLoad = first.isFullLoad();
                    break;
                }
            }

            // allOps is only populated for the incremental path
            List<RowOperation> allOps = new ArrayList<>();
            List<String> keysToRemove = new ArrayList<>();

            if (!isAllFullLoad) {
                // Incremental: drain all thread queues into allOps for global sort + PK merge
                for (String key : sortedKeys) {
                    LinkedBlockingDeque<RowOperation> queue =
                            GlobalSetConfInfo.IceBergSchemaImmuTableOpsMap.get(key);
                    if (queue == null) continue;
                    List<RowOperation> drained = new ArrayList<>();
                    synchronized (queue) {
                        queue.drainTo(drained);
                    }
                    if (!drained.isEmpty()) {
                        allOps.addAll(drained);
                        keysToRemove.add(key);
                    }
                }
                if (allOps.isEmpty()) return;
            } else {
                // Full-load: do NOT drain queues; collect keys for cache cleanup only
                for (String key : sortedKeys) {
                    LinkedBlockingDeque<RowOperation> queue =
                            GlobalSetConfInfo.IceBergSchemaImmuTableOpsMap.get(key);
                    if (queue != null && !queue.isEmpty()) {
                        keysToRemove.add(key);
                    }
                }
                if (keysToRemove.isEmpty()) return;
            }

            int totalOps = isAllFullLoad ? keysToRemove.size() : allOps.size();
            int flushedUntil = 0;

            log.info("[IceBergBatch][tid={}] flush start table={} isAllFullLoad={} keysCount={}",
                    Thread.currentThread().getId(), tableKeyName, isAllFullLoad, keysToRemove.size());

            try {
                if (isAllFullLoad) {
                    // --------------------------------------------------------
                    // Full-load path:
                    //   Stream directly from each thread queue into local Parquet files.
                    //   allOps is intentionally never populated here — this eliminates
                    //   the intermediate List that was the root cause of OOM on wide tables.
                    //   Each record is poll()'d, written, and immediately eligible for GC.
                    // --------------------------------------------------------
                    Table iceBergTable = GlobalSetConfInfo.IceBergCacheTableMap.get(tableKeyName);
                    if (iceBergTable == null) {
                        throw new IllegalStateException("[FullLoad] table not found: " + tableKeyName);
                    }

                    for (String key : keysToRemove) {
                        LinkedBlockingDeque<RowOperation> queue =
                                GlobalSetConfInfo.IceBergSchemaImmuTableOpsMap.get(key);
                        if (queue == null || queue.isEmpty()) continue;

                        int queueSize = queue.size();
                        log.info("[IceBergBatch][tid={}] [FullLoad] stream write start key={} queueSize={}",
                                Thread.currentThread().getId(), key, queueSize);

                        long writeStart = System.currentTimeMillis();
                        List<DataFile> dataFiles = writeFullLoadStreamFromQueue(iceBergTable, queue, tableKeyName);
                        long writeEnd = System.currentTimeMillis();

                        for (DataFile df : dataFiles) {
                            File localFile = new File(df.path().toString());
                            IcebergFullLoadAsyncCommitter.saveToLocalCache(tableKeyName, df, localFile);
                        }

                        flushedUntil++;
                        log.info("[IceBergBatch][tid={}] [FullLoad] stream write done key={} dataFiles={} writeMs={}",
                                Thread.currentThread().getId(), key, dataFiles.size(), writeEnd - writeStart);
                    }

                } else {
                    // --------------------------------------------------------
                    // Incremental path:
                    //   Global sort by binlogOffset guarantees cross-thread CDC order.
                    //   Then split into batches capped by flushBatchMaxSize to prevent
                    //   Iceberg OOM and commit conflicts on large incremental loads.
                    // --------------------------------------------------------
                    allOps.sort(Comparator.comparingLong(RowOperation::getBinlogOffset));

                    int batchMaxSize = Constant.flushBatchMaxSize;
                    log.info("[IceBergBatch][tid={}] [Incremental] flush start table={} totalOps={} batchMaxSize={}",
                            Thread.currentThread().getId(), tableKeyName, allOps.size(), batchMaxSize);

                    while (!allOps.isEmpty()) {
                        int batchSize = Math.min(batchMaxSize, allOps.size());
                        List<RowOperation> batch = new ArrayList<>(allOps.subList(0, batchSize));
                        String batchId = UUID.randomUUID().toString().substring(0, 8);

                        log.info("[IceBergBatch][tid={}] >>> batchId={} table={} [{}-{}/{}]",
                                Thread.currentThread().getId(), batchId, tableKeyName,
                                flushedUntil, flushedUntil + batchSize, totalOps);

                        FlushMetrics metrics = flushBatch(tableKeyName, tableKeyName, batch);
                        flushedUntil += batchSize;

                        allOps.subList(0, batchSize).clear();
                        batch = null;

                        log.info("[IceBergBatch][tid={}] <<< batchId={} table={} dataFiles={} deleteFiles={}",
                                Thread.currentThread().getId(), batchId, tableKeyName,
                                metrics.dataFilesCount, metrics.deleteFilesCount);
                    }
                }

                keysToRemove.forEach(k -> {
                    GlobalSetConfInfo.IceBergTableGnericCacheMap.remove(k);
                    GlobalConfInfo.lastDataWriteTimerByParquetMap.remove(k);
                    GlobalConfInfo.engineAtomicByTableKeyMap.remove(k);
                });

            } catch (Exception e) {
                int remaining = allOps.size();
                log.error("[IceBergBatch][tid={}] Flush failed table={} flushedOps={}/{} remaining={}. Error: {}",
                        Thread.currentThread().getId(), tableKeyName, flushedUntil, totalOps, remaining, e.getMessage());

                // Roll back unprocessed incremental ops to the first queue for retry
                if (!allOps.isEmpty() && !keysToRemove.isEmpty()) {
                    String fallbackKey = keysToRemove.get(0);
                    LinkedBlockingDeque<RowOperation> fallbackQueue =
                            GlobalSetConfInfo.IceBergSchemaImmuTableOpsMap.get(fallbackKey);
                    if (fallbackQueue != null) {
                        synchronized (fallbackQueue) {
                            for (int i = allOps.size() - 1; i >= 0; i--) {
                                fallbackQueue.addFirst(allOps.get(i));
                            }
                        }
                        log.error("[IceBergBatch][tid={}] Rolled back {} ops to key={}",
                                Thread.currentThread().getId(), allOps.size(), fallbackKey);
                    }
                }
                throw e;
            }
        } finally {
            tableLock.unlock();
        }
    }

    // ========== RowDelta commit ==========

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
                if (dataFiles != null) for (DataFile df : dataFiles) rowDelta.addRows(df);
                if (deleteFiles != null) for (DeleteFile df : deleteFiles) rowDelta.addDeletes(df);
                rowDelta.commit();
                int dataFileCount = dataFiles == null ? 0 : dataFiles.size();
                int deleteFileCount = deleteFiles == null ? 0 : deleteFiles.size();
                log.info("[IceBergBatch][tid={}] commit ok key={} attempt={} dataFiles={} deleteFiles={}",
                        Thread.currentThread().getId(), logKey, attempt, dataFileCount, deleteFileCount);
                return;
            } catch (org.apache.iceberg.exceptions.CommitFailedException cfe) {
                log.warn("[IceBergBatch][tid={}] commit conflict retry key={} attempt={}/{} error={}",
                        Thread.currentThread().getId(), logKey, attempt, maxRetry, cfe.getMessage());
                if (attempt >= maxRetry)
                    throw new RuntimeException("[IceBergBatch] commit retry failed key=" + logKey, cfe);
                try {
                    Thread.sleep(200L * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            } catch (org.apache.iceberg.exceptions.ValidationException ve) {
                String dataPaths = dataFiles == null ? "[]"
                        : dataFiles.stream().map(f -> f.path().toString()).collect(Collectors.toList()).toString();
                String deletePaths = deleteFiles == null ? "[]"
                        : deleteFiles.stream().map(f -> f.path().toString()).collect(Collectors.toList()).toString();
                log.error("[IceBergBatch][tid={}] validation failed key={} error={} dataFiles={} deleteFiles={}",
                        Thread.currentThread().getId(), logKey, ve.getMessage(), dataPaths, deletePaths);
                throw new RuntimeException("[IceBergBatch] ValidationException. key=" + logKey, ve);
            }
        }
    }

    // ========== Utility methods ==========

    private static List<Integer> resolveEqualityFieldIds(Table iceBergTable, List<String> pkNames) {
        if (pkNames != null && !pkNames.isEmpty()) {
            List<Integer> ids = pkNames.stream()
                    .map(name -> {
                        Types.NestedField field = iceBergTable.schema().findField(name);
                        if (field == null)
                            throw new IllegalArgumentException("[IceBergBatch] PK col not in schema: " + name);
                        return field.fieldId();
                    })
                    .collect(Collectors.toList());
            log.info("[IceBergBatch][tid={}][TX] equality delete by PK table={} pkNames={} fieldIds={}",
                    Thread.currentThread().getId(), iceBergTable.name(), pkNames, ids);
            return ids;
        } else {
            log.warn("[IceBergBatch][tid={}] no PK, use all cols for equality delete",
                    Thread.currentThread().getId());
            List<Integer> ids = iceBergTable.schema().columns().stream()
                    .map(Types.NestedField::fieldId)
                    .collect(Collectors.toList());
            log.info("[IceBergBatch][tid={}][TX] equality delete by ALL_COLS table={} fieldIds={}",
                    Thread.currentThread().getId(), iceBergTable.name(), ids);
            return ids;
        }
    }

    private static String extractPkKey(RowOperation op, List<String> pkNames) {
        GenericRecord record = chooseRecordForPk(op, pkNames);
        if (record == null)
            throw new IllegalArgumentException("[IceBergBatch] op has no record: " + op);

        StringBuilder sb = PK_SB_CACHE.get();
        sb.setLength(0);

        if (pkNames != null && !pkNames.isEmpty()) {
            for (int i = 0; i < pkNames.size(); i++) {
                if (i > 0) sb.append("|");
                sb.append(safeString(record.getField(pkNames.get(i))));
            }
        } else {
            List<Types.NestedField> fields = record.struct().fields();
            for (int i = 0; i < fields.size(); i++) {
                if (i > 0) sb.append("|");
                sb.append(safeString(record.getField(fields.get(i).name())));
            }
        }
        return sb.toString();
    }

    private static String safeString(Object val) {
        return val == null ? "__NULL__" : val.toString();
    }

    private static GenericRecord chooseRecordForPk(RowOperation op, List<String> pkNames) {
        GenericRecord newRecord = op.getNewRecord();
        GenericRecord oldRecord = op.getOldRecord();
        if (op.getType() == RowOperation.OpType.DELETE) {
            if (hasPkValue(oldRecord, pkNames)) return oldRecord;
            if (hasPkValue(newRecord, pkNames)) return newRecord;
            return oldRecord != null ? oldRecord : newRecord;
        }
        if (hasPkValue(newRecord, pkNames)) return newRecord;
        if (hasPkValue(oldRecord, pkNames)) return oldRecord;
        return newRecord != null ? newRecord : oldRecord;
    }

    private static boolean hasPkValue(GenericRecord record, List<String> pkNames) {
        if (record == null) return false;
        if (pkNames == null || pkNames.isEmpty()) return true;
        for (String pk : pkNames) {
            Object v = record.getField(pk);
            if (v == null || String.valueOf(v).isEmpty()) return false;
        }
        return true;
    }

    private static String summarizeRecords(List<GenericRecord> records, List<String> keys, int limit) {
        if (records == null || records.isEmpty()) return "[]";
        StringBuilder sb = new StringBuilder("[");
        int max = Math.min(records.size(), limit);
        for (int i = 0; i < max; i++) {
            if (i > 0) sb.append(", ");
            sb.append(extractRecordKey(records.get(i), keys));
        }
        if (records.size() > limit) {
            sb.append(", ...+").append(records.size() - limit);
        }
        sb.append("]");
        return sb.toString();
    }

    private static String extractRecordKey(GenericRecord record, List<String> keys) {
        if (record == null) return "__NULL_RECORD__";
        List<String> useKeys = keys;
        if (useKeys == null || useKeys.isEmpty()) {
            useKeys = new ArrayList<>();
            for (Types.NestedField f : record.struct().fields()) {
                useKeys.add(f.name());
            }
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < useKeys.size(); i++) {
            if (i > 0) sb.append(",");
            String k = useKeys.get(i);
            sb.append(k).append("=").append(safeString(record.getField(k)));
        }
        return sb.toString();
    }

    private static class MergeResult {
        final List<GenericRecord> insertRecords;
        final List<GenericRecord> deleteRecords;

        MergeResult(List<GenericRecord> insertRecords, List<GenericRecord> deleteRecords) {
            this.insertRecords = insertRecords;
            this.deleteRecords = deleteRecords;
        }
    }

    // ========== Full-load stream write (core OOM fix) ==========

    /**
     * Full-load dedicated streaming write.
     *
     * Design principle:
     *   Never create an intermediate List. Poll records directly from the queue,
     *   write to Parquet, and release the reference immediately.
     *   For 600-column wide tables a single GenericRecord can be hundreds of KB;
     *   any form of batch accumulation will blow the heap.
     *   At any point in time only two things live on the heap:
     *     1. The Parquet FileAppender RowGroup Buffer (tuned by column count)
     *     2. The single GenericRecord currently being written (dereferenced after write)
     *
     * @param table        Target Iceberg table
     * @param queue        Source queue for full-load data; consumed to empty by this method
     * @param tableKeyName Table key used for logging and path construction
     */
    public static List<DataFile> writeFullLoadStreamFromQueue(
            Table table,
            LinkedBlockingDeque<RowOperation> queue,
            String tableKeyName) throws Exception {

        if (queue == null || queue.isEmpty()) return Collections.emptyList();

        PartitionSpec spec = table.spec();
        boolean isPartitioned = spec.isPartitioned();
        int colCount = table.schema().columns().size();

        // Adaptive Parquet memory tuning based on column count.
        // The more columns, the wider each row, so the RowGroup Buffer must be smaller
        // to avoid it alone consuming too much heap.
        String rowGroupSize;
        String pageSize;
        if (colCount > 400) {
            rowGroupSize = "8388608";   // 8MB — for 600-column tables
            pageSize     = "131072";    // 128KB
            log.info("[FullLoadStream][tid={}] Extra-wide table (cols={}), Parquet RowGroup set to 8MB",
                    Thread.currentThread().getId(), colCount);
        } else if (colCount > 100) {
            rowGroupSize = "16777216";  // 16MB
            pageSize     = "262144";    // 256KB
            log.info("[FullLoadStream][tid={}] Wide table (cols={}), Parquet RowGroup set to 16MB",
                    Thread.currentThread().getId(), colCount);
        } else {
            rowGroupSize = "67108864";  // 64MB — normal tables
            pageSize     = "524288";    // 512KB
        }

        File cacheDir = new File(Constant.basicWorkPath, "FileCache/" + tableKeyName);
        if (!cacheDir.exists()) cacheDir.mkdirs();

        if (!isPartitioned) {
            return streamToSingleFile(table, queue, spec, cacheDir, rowGroupSize, pageSize, tableKeyName);
        }
        return streamToPartitionedFiles(table, queue, spec, cacheDir, rowGroupSize, pageSize, tableKeyName);
    }

    /**
     * Non-partitioned table: single Parquet writer, poll row by row from queue.
     */
    /*private static List<DataFile> streamToSingleFile(
            Table table,
            LinkedBlockingDeque<RowOperation> queue,
            PartitionSpec spec,
            File cacheDir,
            String rowGroupSize,
            String pageSize,
            String tableKeyName) throws Exception {

        File localFile = new File(cacheDir, UUID.randomUUID() + ".parquet");
        org.apache.iceberg.io.OutputFile outputFile = org.apache.iceberg.Files.localOutput(localFile);

        FileAppender<GenericRecord> appender = Parquet.write(outputFile)
                .schema(table.schema())
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .set("write.parquet.compression-codec", "snappy")
                .set("write.parquet.row-group-size-bytes", rowGroupSize)
                .set("write.parquet.page-size-bytes", pageSize)
                .build();

        int written = 0;
        try {
            RowOperation op;
            // poll rather than drain: after each write the record has no strong reference
            // and can be reclaimed by GC at the next YGC cycle
            while ((op = queue.pollFirst()) != null) {
                GenericRecord rec = op.getNewRecord();
                if (rec != null) {
                    appender.add(rec);
                    written++;
                }
            }
        } finally {
            appender.close();
            log.info("[FullLoadStream][tid={}] non-partitioned write done table={} rows={}",
                    Thread.currentThread().getId(), tableKeyName, written);
        }

        return Collections.singletonList(
                DataFiles.builder(spec)
                        .withInputFile(outputFile.toInputFile())
                        .withMetrics(appender.metrics())
                        .withFormat(FileFormat.PARQUET)
                        .build()
        );
    }

    *//**
     * Partitioned table: maintain one writer per partition key, poll row by row from queue.
     *//*
    private static List<DataFile> streamToPartitionedFiles(
            Table table,
            LinkedBlockingDeque<RowOperation> queue,
            PartitionSpec spec,
            File cacheDir,
            String rowGroupSize,
            String pageSize,
            String tableKeyName) throws Exception {

        Map<PartitionKey, WriterContext> writersMap = new HashMap<>();
        PartitionKey reusablePKey = new PartitionKey(spec, table.schema());
        int written = 0;

        try {
            RowOperation op;
            while ((op = queue.pollFirst()) != null) {
                GenericRecord rec = op.getNewRecord();
                if (rec == null) continue;

                reusablePKey.partition(rec);
                PartitionKey currentKey = reusablePKey.copy();

                WriterContext ctx = writersMap.get(currentKey);
                if (ctx == null) {
                    File localFile = new File(cacheDir, UUID.randomUUID() + ".parquet");
                    org.apache.iceberg.io.OutputFile outputFile = org.apache.iceberg.Files.localOutput(localFile);
                    FileAppender<GenericRecord> appender = Parquet.write(outputFile)
                            .schema(table.schema())
                            .createWriterFunc(GenericParquetWriter::buildWriter)
                            .set("write.parquet.compression-codec", "snappy")
                            .set("write.parquet.row-group-size-bytes", rowGroupSize)
                            .set("write.parquet.page-size-bytes", pageSize)
                            .build();
                    ctx = new WriterContext(localFile, outputFile, appender);
                    writersMap.put(currentKey, ctx);
                }
                ctx.appender.add(rec);
                written++;
                // op and rec strong references end here; GC can reclaim them
            }
        } finally {
            log.info("[FullLoadStream][tid={}] partitioned write done table={} rows={} partitions={}",
                    Thread.currentThread().getId(), tableKeyName, written, writersMap.size());
        }

        List<DataFile> result = new ArrayList<>();
        for (Map.Entry<PartitionKey, WriterContext> entry : writersMap.entrySet()) {
            WriterContext ctx = entry.getValue();
            try {
                ctx.appender.close();
                result.add(DataFiles.builder(spec)
                        .withInputFile(ctx.outputFile.toInputFile())
                        .withMetrics(ctx.appender.metrics())
                        .withFormat(FileFormat.PARQUET)
                        .withPartition(entry.getKey())
                        .build());
            } catch (Exception e) {
                log.error("[FullLoadStream] Failed to close writer for partition={} table={}",
                        entry.getKey(), tableKeyName, e);
            }
        }
        writersMap.clear();
        return result;
    }
    *//**
     * Non-partitioned table: split into multiple small files, one per N rows.
     * For wide tables the Parquet FileAppender holds one column-writer buffer per column.
     * 100 columns means 100 concurrent buffers inside the writer — keeping one appender
     * open for all rows means all those buffers live on the heap simultaneously.
     * Closing and reopening every rowsPerFile rows bounds the heap to a predictable ceiling.
     */
    private static List<DataFile> streamToSingleFile(
            Table table,
            LinkedBlockingDeque<RowOperation> queue,
            PartitionSpec spec,
            File cacheDir,
            String rowGroupSize,
            String pageSize,
            String tableKeyName) throws Exception {

        int colCount = table.schema().columns().size();
        // The wider the table, the fewer rows per file.
        // Each Parquet column writer holds its own encoding buffer;
        // closing the file flushes and releases all of them at once.
        int rowsPerFile = resolveRowsPerFile(colCount);
        log.info("[FullLoadStream][tid={}] non-partitioned table={} cols={} rowsPerFile={}",
                Thread.currentThread().getId(), tableKeyName, colCount, rowsPerFile);

        List<DataFile> result = new ArrayList<>();
        int totalWritten = 0;
        int fileIndex = 0;

        while (!queue.isEmpty()) {
            File localFile = new File(cacheDir, UUID.randomUUID() + ".parquet");
            org.apache.iceberg.io.OutputFile outputFile =
                    org.apache.iceberg.Files.localOutput(localFile);

            FileAppender<GenericRecord> appender = Parquet.write(outputFile)
                    .schema(table.schema())
                    .createWriterFunc(GenericParquetWriter::buildWriter)
                    .set("write.parquet.compression-codec", "snappy")
                    .set("write.parquet.row-group-size-bytes", rowGroupSize)
                    .set("write.parquet.page-size-bytes", pageSize)
                    .build();

            int writtenThisFile = 0;
            try {
                RowOperation op;
                while (writtenThisFile < rowsPerFile && (op = queue.pollFirst()) != null) {
                    GenericRecord rec = op.getNewRecord();
                    if (rec != null) {
                        appender.add(rec);
                        writtenThisFile++;
                        totalWritten++;
                    }
                    // op and rec dereferenced here; eligible for GC immediately
                }
            } finally {
                // Closing the appender flushes the final RowGroup and releases
                // ALL column-writer buffers — this is the key memory release point
                appender.close();
            }

            if (writtenThisFile > 0) {
                result.add(DataFiles.builder(spec)
                        .withInputFile(outputFile.toInputFile())
                        .withMetrics(appender.metrics())
                        .withFormat(FileFormat.PARQUET)
                        .build());
                fileIndex++;
                log.info("[FullLoadStream][tid={}] file#{} closed table={} rowsThisFile={} totalWritten={}",
                        Thread.currentThread().getId(), fileIndex, tableKeyName,
                        writtenThisFile, totalWritten);
            }
        }

        log.info("[FullLoadStream][tid={}] non-partitioned write done table={} totalRows={} files={}",
                Thread.currentThread().getId(), tableKeyName, totalWritten, result.size());
        return result;
    }

    /**
     * Partitioned table: same row-count cap per file, per partition.
     */
    private static List<DataFile> streamToPartitionedFiles(
            Table table,
            LinkedBlockingDeque<RowOperation> queue,
            PartitionSpec spec,
            File cacheDir,
            String rowGroupSize,
            String pageSize,
            String tableKeyName) throws Exception {

        int colCount = table.schema().columns().size();
        int rowsPerFile = resolveRowsPerFile(colCount);
        log.info("[FullLoadStream][tid={}] partitioned table={} cols={} rowsPerFile={}",
                Thread.currentThread().getId(), tableKeyName, colCount, rowsPerFile);

        List<DataFile> result = new ArrayList<>();
        PartitionKey reusablePKey = new PartitionKey(spec, table.schema());

        // Current open writers per partition
        Map<PartitionKey, WriterContext> writersMap = new HashMap<>();
        // Row count per partition writer
        Map<PartitionKey, Integer> writerRowCount = new HashMap<>();

        int totalWritten = 0;

        try {
            RowOperation op;
            while ((op = queue.pollFirst()) != null) {
                GenericRecord rec = op.getNewRecord();
                if (rec == null) continue;

                reusablePKey.partition(rec);
                PartitionKey currentKey = reusablePKey.copy();

                // Check if current writer for this partition has hit the row cap
                Integer rowCount = writerRowCount.getOrDefault(currentKey, 0);
                if (rowCount >= rowsPerFile) {
                    // Close this partition's writer, collect the DataFile, open a new one
                    WriterContext oldCtx = writersMap.remove(currentKey);
                    writerRowCount.remove(currentKey);
                    if (oldCtx != null) {
                        oldCtx.appender.close();
                        result.add(DataFiles.builder(spec)
                                .withInputFile(oldCtx.outputFile.toInputFile())
                                .withMetrics(oldCtx.appender.metrics())
                                .withFormat(FileFormat.PARQUET)
                                .withPartition(currentKey)
                                .build());
                        log.info("[FullLoadStream][tid={}] partition file rolled table={} partition={} rows={}",
                                Thread.currentThread().getId(), tableKeyName, currentKey, rowCount);
                    }
                }

                // Get or create writer for this partition
                WriterContext ctx = writersMap.get(currentKey);
                if (ctx == null) {
                    File localFile = new File(cacheDir, UUID.randomUUID() + ".parquet");
                    org.apache.iceberg.io.OutputFile outputFile =
                            org.apache.iceberg.Files.localOutput(localFile);
                    FileAppender<GenericRecord> appender = Parquet.write(outputFile)
                            .schema(table.schema())
                            .createWriterFunc(GenericParquetWriter::buildWriter)
                            .set("write.parquet.compression-codec", "snappy")
                            .set("write.parquet.row-group-size-bytes", rowGroupSize)
                            .set("write.parquet.page-size-bytes", pageSize)
                            .build();
                    ctx = new WriterContext(localFile, outputFile, appender);
                    writersMap.put(currentKey, ctx);
                    writerRowCount.put(currentKey, 0);
                }

                ctx.appender.add(rec);
                writerRowCount.put(currentKey, writerRowCount.get(currentKey) + 1);
                totalWritten++;
            }
        } finally {
            // Close all remaining open writers
            for (Map.Entry<PartitionKey, WriterContext> entry : writersMap.entrySet()) {
                PartitionKey pKey = entry.getKey();
                WriterContext ctx = entry.getValue();
                try {
                    ctx.appender.close();
                    result.add(DataFiles.builder(spec)
                            .withInputFile(ctx.outputFile.toInputFile())
                            .withMetrics(ctx.appender.metrics())
                            .withFormat(FileFormat.PARQUET)
                            .withPartition(pKey)
                            .build());
                } catch (Exception e) {
                    log.error("[FullLoadStream] Failed to close writer partition={} table={}",
                            pKey, tableKeyName, e);
                }
            }
            writersMap.clear();
            writerRowCount.clear();
            log.info("[FullLoadStream][tid={}] partitioned write done table={} totalRows={} files={}",
                    Thread.currentThread().getId(), tableKeyName, totalWritten, result.size());
        }
        return result;
    }

    /**
     * Determine how many rows to write per Parquet file based on column count.
     * Wider tables need smaller files because each column writer holds its own
     * encoding buffer inside the Parquet FileAppender. More columns = more
     * concurrent buffers on the heap. Closing the file releases all of them.
     */
    /**
     * 遍历 GlobalSetConfInfo.fullLoadDirectWriterMap，寻找属于本表的所有 threadId 对应的 writer。
     * 调用 rollAndSaveDirectWriter 关闭文件并提交。
     */
    public static void flushDirectWriters(String tableKeyName) {
        String prefix = tableKeyName + ".";
        List<String> threadKeysToFlush = new java.util.ArrayList<>();

        // 1. 识别属于当前表的所有活跃 writer key
        for (String threadKey : GlobalSetConfInfo.fullLoadDirectWriterMap.keySet()) {
            if (threadKey.startsWith(prefix)) {
                threadKeysToFlush.add(threadKey);
            }
        }

        if (threadKeysToFlush.isEmpty()) return;

        log.info("[IceBergBatch][tid={}] flushing {} direct writers for table={}",
                Thread.currentThread().getId(), threadKeysToFlush.size(), tableKeyName);

        // 2. 借用 ThreadPool 类里的 roll 方法执行关闭逻辑
        for (String threadKey : threadKeysToFlush) {
            try {
                // 如果 owner 线程正在写入，本次跳过，等下一个定时器周期再处理，避免并发关闭 appender
                GlobalSetConfInfo.FullLoadDirectWriter writer =
                        GlobalSetConfInfo.fullLoadDirectWriterMap.get(threadKey);
                if (writer != null && writer.activelyWriting) {
                    log.info("[DirectWrite] flushDirectWriters skip active writer key={}", threadKey);
                    continue;
                }

                com.jddm.thread.OperationTotalSyncByIceBergThreadPool.rollAndSaveDirectWriter(
                        threadKey, tableKeyName);
            } catch (Exception e) {
                log.error("[IceBergBatch][tid={}] flush direct writer failed key={} err={}",
                        Thread.currentThread().getId(), threadKey, e.getMessage());
            }
        }
    }

    private static int resolveRowsPerFile(int colCount) {
        if (colCount > 400) return 500;   // 600-col table: close file every 500 rows
        if (colCount > 100) return 2000;  // 100-col table: close file every 2000 rows
        return 10000;                     // normal table: close file every 10000 rows
    }
}