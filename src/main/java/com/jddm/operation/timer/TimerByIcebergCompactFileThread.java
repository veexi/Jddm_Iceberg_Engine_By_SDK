package com.jddm.operation.timer;

import com.jddm.common.Constant;
import com.jddm.conf.GlobalSetConfInfo;
import com.jddm.operation.IceBergBatchOperationHandler;
import org.apache.iceberg.*;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

/**
 * 每小时扫描所有 Iceberg 表，合并碎小文件，过期旧快照。
 */
public class TimerByIcebergCompactFileThread implements Runnable {

    private static final Logger log = LogManager.getLogger(TimerByIcebergCompactFileThread.class);

    /** 至少达到该数量的小文件才触发合并 */
    private static final int MIN_FILES_TO_COMPACT =
            Integer.getInteger("compact.min.files", 3);

    /** 快照保留最近 N 个 */
    private static final int SNAPSHOTS_TO_RETAIN = 5;

    @Override
    public void run() {
        log.info("[Compact] hourly scan start, tables={}",
                GlobalSetConfInfo.IceBergCacheTableMap.size());
        for (Map.Entry<String, Table> entry : GlobalSetConfInfo.IceBergCacheTableMap.entrySet()) {
            try {
                compactTable(entry.getKey(), entry.getValue());
            } catch (Exception e) {
                log.error("[Compact] table={} failed", entry.getKey(), e);
            }
        }
        log.info("[Compact] hourly scan done");
    }

    private void compactTable(String tableKey, Table table) throws Exception {

        if (table.currentSnapshot() == null) {
            log.info("[Compact] table={} no snapshot yet, skip", tableKey);
            return;
        }

        Schema schema = table.schema();

        List<FileScanTask> smallTasks    = new ArrayList<>();
        Set<DataFile>      filesToRemove = new HashSet<>();

        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                if (task.file().fileSizeInBytes() < Constant.compactSmallFileSizeBytes) {
                    smallTasks.add(task);
                    filesToRemove.add(task.file());
                }
            }
        }

        if (smallTasks.size() < MIN_FILES_TO_COMPACT) {
            log.info("[Compact] table={} smallFiles={}, need>={}, skip",
                    tableKey, smallTasks.size(), MIN_FILES_TO_COMPACT);
            return;
        }
        log.info("[Compact] table={} found {} small files, start compact", tableKey, smallTasks.size());

        List<GenericRecord> allRecords = new ArrayList<>();
        // 核心修复：使用 IcebergGenerics.read(table) 进行读取。
        // 该方法会应用所有的 DeleteFiles，确保读出的数据是业务逻辑上“活着”的行。
        // 直接读取 Parquet 物理文件会导致已删除数据“复活”。
        try (CloseableIterable<Record> reader = IcebergGenerics.read(table).build()) {
            for (Record rec : reader) {
                allRecords.add((GenericRecord) rec);
            }
        }

        if (allRecords.isEmpty()) {
            log.warn("[Compact] table={} all small files empty, nothing to write, skip", tableKey);
            return;
        }

        String newFilePath = table.location() + "/data/compact-"
                + UUID.randomUUID() + ".parquet";
        OutputFile outputFile = table.io().newOutputFile(newFilePath);

        DataFile newDataFile = IceBergBatchOperationHandler
                .writeDataFile(table, allRecords, tableKey);
        try {
            table.newRewrite()
                    .rewriteFiles(filesToRemove, Collections.singleton(newDataFile))
                    .commit();
            log.info("[Compact] table={} rewrite ok: {} files -> 1 file, rows={}, newSize={} bytes",
                    tableKey, filesToRemove.size(), allRecords.size(), newDataFile.fileSizeInBytes());
        } catch (org.apache.iceberg.exceptions.ValidationException ve) {
            log.warn("[Compact] table={} skip, concurrent delete conflict, retry next cycle. msg={}",
                    tableKey, ve.getMessage());
            return;
        }

        table.expireSnapshots()
                .expireOlderThan(System.currentTimeMillis() - 24L * 3600 * 1000)
                .retainLast(SNAPSHOTS_TO_RETAIN)
                .commit();
        log.info("[Compact] table={} expire snapshots done, retainLast={}", tableKey, SNAPSHOTS_TO_RETAIN);
    }
}