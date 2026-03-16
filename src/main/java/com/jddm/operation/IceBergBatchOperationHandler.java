package com.jddm.operation;

import com.jddm.common.Constant;
import com.jddm.conf.GlobalConfInfo;
import com.jddm.conf.GlobalSetConfInfo;
import com.jddm.vo.RowOperation;
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
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;

/**
 * Iceberg 批次操作处理器：负责对 CDC 数据进行主键级合并、写入数据/删除文件，并提交到 Iceberg 表。
 * 支持多种写入模式：Trajectory（轨迹模式）、COW（写时复制）以及 RowDelta（行增量模式）。
 */
public class IceBergBatchOperationHandler {

    private static final Logger log = LogManager.getLogger(IceBergBatchOperationHandler.class);

    private static final Map<String, java.util.concurrent.locks.ReentrantLock> tableFlushLocks =
            new java.util.concurrent.ConcurrentHashMap<>();

    public static class FlushMetrics {
        public final int dataFilesCount;    // 写入的数据文件数量
        public final int deleteFilesCount;  // 写入的删除文件（Equality Delete）数量
        public final java.util.List<String> dataFilePaths;   // 数据文件路径列表
        public final java.util.List<String> deleteFilePaths; // 删除文件路径列表

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

        // 无主键表处理逻辑：走轨迹模式（Audit records），所有 I/U/D 操作都直接转化为 Append
        if (pkNames == null || pkNames.isEmpty()) {
            log.info("[IceBergBatch][tid={}][Trajectory] No PK table, writing all ops as audit records, table={} ops={}",
                    Thread.currentThread().getId(), tableKeyName, allOps.size());

            // 将所有操作记录展开为待写入的记录列表
            List<GenericRecord> recordsToWrite = allOps.stream()
                    .flatMap(op -> {
                        List<GenericRecord> rows = new ArrayList<>();
                        switch (op.getType()) {
                            case INSERT:
                                if (op.getNewRecord() != null) rows.add(op.getNewRecord());
                                break;
                            case DELETE:
                                // 轨迹模式下，删除也记录其原有数据
                                if (op.getOldRecord() != null) rows.add(op.getOldRecord());
                                break;
                            case UPDATE:
                                // 更新记录则同时保留旧镜像和新镜像
                                if (op.getOldRecord() != null) rows.add(op.getOldRecord());
                                if (op.getNewRecord() != null) rows.add(op.getNewRecord());
                                break;
                        }
                        return rows.stream();
                    })
                    .collect(Collectors.toList());

            if (recordsToWrite.isEmpty())
                return new FlushMetrics(0, 0, Collections.emptyList(), Collections.emptyList());

            // 写入分区数据文件
            List<DataFile> dataFiles = writePartitionedDataFiles(iceBergTable, recordsToWrite, tableKeyName);
            List<String> dataPaths = dataFiles.stream().map(df -> df.path().toString()).collect(Collectors.toList());

            // 提交追加操作
            AppendFiles appendFiles = iceBergTable.newAppend();
            for (DataFile df : dataFiles) appendFiles.appendFile(df);
            appendFiles.commit();

            log.info("[IceBergBatch][tid={}][Trajectory] commit ok key={} rows={} records={} dataFiles={}",
                    Thread.currentThread().getId(), tableKeyName, allOps.size(), recordsToWrite.size(), dataPaths);
            return new FlushMetrics(dataFiles.size(), 0, dataPaths, Collections.emptyList());
        }

        if ("transaction".equals(Constant.icebergWriteMode)) {

            // ============================================================
            // COW 模式（Copy-On-Write）：实时合并模式
            // 每批次写入时直接扫描并读取受影响的旧文件，在内存中过滤/更新后重写全文件。
            // 优点：查询效率极高（无 delete file），适用于不支持 Merge-on-Read 的引擎（如 Hive 3.x）。
            // 缺点：由于存在写放大，适合低频、大批量的写入场景。
            // ============================================================
            if ("batch".equals(Constant.cowMode)) {
                log.info("[IceBergBatch][tid={}][COW] flush start table={} ops={} pkNames={}",
                        Thread.currentThread().getId(), tableKeyName, allOps.size(), pkNames);

                MergeResult mergeResult = mergeByPrimaryKey(allOps, pkNames, iceBergTable, tableKeyName);
                log.info("[IceBergBatch][tid={}][COW] merge done table={} insertCount={} deleteCount={}",
                        Thread.currentThread().getId(), tableKeyName,
                        mergeResult.insertRecords.size(), mergeResult.deleteRecords.size());

                // 1. 收集本次所有涉及的 PK（insert + delete 两侧）
                Set<String> affectedPks = new HashSet<>();
                for (GenericRecord r : mergeResult.insertRecords) {
                    affectedPks.add(buildPkString(r, pkNames));
                }
                for (GenericRecord r : mergeResult.deleteRecords) {
                    affectedPks.add(buildPkString(r, pkNames));
                }

                // 2. 扫描 Iceberg 现有数据，只读出包含 affected PK 的文件，过滤掉这些 PK
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
                                    // 被覆盖或删除，不保留
                                } else {
                                    kept.add(rec);
                                }
                            }
                            if (hasMatch) {
                                survivingRecords.addAll(kept);
                                oldDataFiles.add(task.file());
                            }
                            // 没有命中的文件直接跳过，不动它
                        }
                    }
                }

                // 3. 合并：surviving 旧数据 + 本次新 insert（纯 DELETE 的 PK 已被过滤掉）
                survivingRecords.addAll(mergeResult.insertRecords);

                log.info("[IceBergBatch][tid={}][COW] table={} oldDataFiles={} survivingRecords={} newInserts={}",
                        Thread.currentThread().getId(), tableKeyName,
                        oldDataFiles.size(), survivingRecords.size() - mergeResult.insertRecords.size(),
                        mergeResult.insertRecords.size());

                // 4. 没有旧文件被命中：直接 Append 新数据
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

                // 5. 将受影响的旧数据 + 本次新插入数据进行合并，并写入新的 Data File
                List<DataFile> newDataFiles = survivingRecords.isEmpty()
                        ? Collections.emptyList()
                        : writePartitionedDataFiles(iceBergTable, survivingRecords, tableKeyName);
                List<String> dataPaths = newDataFiles.stream()
                        .map(df -> df.path().toString()).collect(Collectors.toList());

                // 6. OverwriteFiles 原子提交：确保删除旧文件的同时增加新文件
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
                // RowDelta 模式（Row-level Delta / Merge-on-Read）：高性能写入模式
                // 写入数据对应的 Data File 以及删除对应 PK 的 Equality Delete File。
                // 适用于高频率、低延迟的数据同步需求。去重逻辑可交给查询引擎或定时任务（Compact）。
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
            // trajectory 模式：纯 Append
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

    // ========== COW 辅助方法 ==========

    public static String buildPkString(GenericRecord r, List<String> pkNames) {
        return pkNames.stream()
                .map(pk -> r.getField(pk) == null ? "__NULL__" : r.getField(pk).toString())
                .collect(Collectors.joining("|"));
    }

    /**
     * 读取指定扫描任务中的全部数据（Parquet），并转换为 GenericRecord 列表。
     */
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

        // 将合并后的结果按操作类型分流，准备进入写入阶段
        for (RowOperation op : mergedMap.values()) {
            switch (op.getType()) {
                case INSERT:
                    finalInserts.add(op.getNewRecord());
                    // 即使是 INSERT，也尝试先按主键删除已有行（Blind Upsert 保证），确保唯一性
                    finalDeletes.add(op.getNewRecord()); 
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
                        // UPDATE 在 Iceberg 底层通常映射为 DELETE 旧 PK + INSERT 新数据
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

    // ========== 文件写入 ==========

    /**
     * 将给定的数据记录同步写入到一个单一的数据文件中（非分区感知，通常用于 Compact 或简单表）。
     */
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
                appender.addAll(records);
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

    /**
     * 核心写入逻辑：根据分区规格（PartitionSpec）将数据拆分到不同的分区路径下，并并行/顺序写入多个数据文件。
     */
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
            String pathStr = new Path(table.location(), "data/" + tableKeyName.replace(".", "/")
                    + "/" + UUID.randomUUID() + ".parquet").toString();
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
                appender.addAll(entry.getValue());
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

    // ========== flushAllThreadsForTable ==========

    public static void flushAllThreadsForTable(String tableKeyName) throws Exception {
        java.util.concurrent.locks.ReentrantLock tableLock =
                tableFlushLocks.computeIfAbsent(tableKeyName,
                        k -> new java.util.concurrent.locks.ReentrantLock());
        tableLock.lock();
        try {
            List<RowOperation> allOps = new ArrayList<>();
            List<String> keysToRemove = new ArrayList<>();

            List<String> sortedKeys = GlobalSetConfInfo.IceBergSchemaImmuTableOpsMap.keySet().stream()
                    .filter(k -> k.startsWith(tableKeyName + "."))
                    .sorted()
                    .collect(Collectors.toList());

            for (String key : sortedKeys) {
                java.util.concurrent.LinkedBlockingDeque<RowOperation> queue =
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

            allOps.sort(Comparator.comparingLong(RowOperation::getBinlogOffset));

            int totalOps = allOps.size();
            int batchMaxSize = Constant.flushBatchMaxSize;
            int flushedUntil = 0;

            log.info("[IceBergBatch][tid={}] flush start table={} totalOps={} batchMaxSize={}",
                    Thread.currentThread().getId(), tableKeyName, totalOps, batchMaxSize);

            try {
                for (int start = 0; start < totalOps; start += batchMaxSize) {
                    int end = Math.min(start + batchMaxSize, totalOps);
                    List<RowOperation> batch = allOps.subList(start, end);
                    String batchId = UUID.randomUUID().toString().substring(0, 8);

                    log.info("[IceBergBatch][tid={}] >>> batchId={} table={} [{}-{}/{}]",
                            Thread.currentThread().getId(), batchId, tableKeyName, start, end, totalOps);

                    FlushMetrics metrics = flushBatch(tableKeyName, tableKeyName, batch);
                    flushedUntil = end;

                    log.info("[IceBergBatch][tid={}] <<< batchId={} table={} dataFiles={} deleteFiles={}",
                            Thread.currentThread().getId(), batchId, tableKeyName,
                            metrics.dataFilesCount, metrics.deleteFilesCount);
                }

                keysToRemove.forEach(k -> {
                    GlobalSetConfInfo.IceBergTableGnericCacheMap.remove(k);
                    GlobalConfInfo.lastDataWriteTimerByParquetMap.remove(k);
                    GlobalConfInfo.engineAtomicByTableKeyMap.remove(k);
                });

            } catch (Exception e) {
                int remaining = totalOps - flushedUntil;
                log.error("[IceBergBatch][tid={}] Flush failed table={} flushedOps={}/{} remaining={}. Error: {}",
                        Thread.currentThread().getId(), tableKeyName, flushedUntil, totalOps, remaining, e.getMessage());

                if (remaining > 0 && !keysToRemove.isEmpty()) {
                    String fallbackKey = keysToRemove.get(0);
                    java.util.concurrent.LinkedBlockingDeque<RowOperation> fallbackQueue =
                            GlobalSetConfInfo.IceBergSchemaImmuTableOpsMap.get(fallbackKey);
                    if (fallbackQueue != null) {
                        List<RowOperation> toRestore = new ArrayList<>(allOps.subList(flushedUntil, totalOps));
                        synchronized (fallbackQueue) {
                            for (int i = toRestore.size() - 1; i >= 0; i--) {
                                fallbackQueue.addFirst(toRestore.get(i));
                            }
                        }
                        log.error("[IceBergBatch][tid={}] Rolled back {} ops to key={}",
                                Thread.currentThread().getId(), toRestore.size(), fallbackKey);
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

    // ========== 工具方法 ==========

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
        List<String> out = new ArrayList<>();
        int max = Math.min(records.size(), limit);
        for (int i = 0; i < max; i++) out.add(extractRecordKey(records.get(i), keys));
        if (records.size() > limit) out.add("...+" + (records.size() - limit));
        return out.toString();
    }

    private static String extractRecordKey(GenericRecord record, List<String> keys) {
        if (record == null) return "__NULL_RECORD__";
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