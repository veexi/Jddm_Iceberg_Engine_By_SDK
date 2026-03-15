package com.jddm.operation.timer;

import com.jddm.common.Constant;
import com.jddm.conf.GlobalSetConfInfo;
import com.jddm.operation.IceBergBatchOperationHandler;
import org.apache.iceberg.*;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 定时任务：扫描所有 Iceberg 表，按配置执行去重合并和/或小文件合并，并过期旧快照。
 *
 * 由 config.properties 参数控制行为：
 *   COMPACT_DEDUP_ENABLED=true/false       是否执行去重（cowMode=timer 时使用）
 *   COMPACT_SMALL_FILES_ENABLED=true/false 是否执行小文件合并
 *   COMPACT_INTERVAL_SECONDS=3600          定时间隔（秒）
 *   HIVE_FILE_CACHE_SIZE=128               小文件阈值（MB）
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
        log.info("[Compact] scan start tables={} dedupEnabled={} smallFilesEnabled={} cowMode={}",
                GlobalSetConfInfo.IceBergCacheTableMap.size(),
                Constant.compactDedupEnabled,
                Constant.compactSmallFilesEnabled,
                Constant.cowMode);

        for (Map.Entry<String, Table> entry : GlobalSetConfInfo.IceBergCacheTableMap.entrySet()) {
            try {
                compactTable(entry.getKey(), entry.getValue());
            } catch (Exception e) {
                log.error("[Compact] table={} failed", entry.getKey(), e);
            }
        }
        log.info("[Compact] scan done");
    }

    private void compactTable(String tableKey, Table table) throws Exception {

        if (table.currentSnapshot() == null) {
            log.info("[Compact] table={} no snapshot yet, skip", tableKey);
            return;
        }

        boolean doDedup = Constant.compactDedupEnabled;
        boolean doSmallFiles = Constant.compactSmallFilesEnabled;

        if (!doDedup && !doSmallFiles) {
            log.info("[Compact] table={} both dedup and smallFiles disabled, skip", tableKey);
            return;
        }

        List<String> pkNames = GlobalSetConfInfo.TablePkColCacheMap.get(tableKey);
        boolean hasPk = pkNames != null && !pkNames.isEmpty();

        // ===== 收集所有文件，筛出小文件 =====
        List<FileScanTask> allTasks = new ArrayList<>();
        List<FileScanTask> smallTasks = new ArrayList<>();

        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                allTasks.add(task);
                if (doSmallFiles && task.file().fileSizeInBytes() < Constant.compactSmallFileSizeBytes) {
                    smallTasks.add(task);
                }
            }
        }

        boolean needCompact = doSmallFiles && smallTasks.size() >= MIN_FILES_TO_COMPACT;
        boolean needDedup = doDedup && hasPk;

        if (!needCompact && !needDedup) {
            log.info("[Compact] table={} smallFiles={} (need>={}), dedup={} hasPk={}, nothing to do",
                    tableKey, smallTasks.size(), MIN_FILES_TO_COMPACT, doDedup, hasPk);
            return;
        }

        log.info("[Compact] table={} needCompact={} smallFiles={} needDedup={}",
                tableKey, needCompact, smallTasks.size(), needDedup);

        // ===== 读取全表当前数据（IcebergGenerics 会自动应用 delete file） =====
        List<GenericRecord> allRecords = new ArrayList<>();
        try (CloseableIterable<Record> reader = IcebergGenerics.read(table).build()) {
            for (Record rec : reader) {
                allRecords.add((GenericRecord) rec);
            }
        }

        if (allRecords.isEmpty()) {
            log.warn("[Compact] table={} no records after reading, skip", tableKey);
            return;
        }

        // ===== 去重（按 PK 保留最后一条，顺序即为读出的顺序） =====
        if (needDedup) {
            LinkedHashMap<String, GenericRecord> dedupMap = new LinkedHashMap<>();
            for (GenericRecord rec : allRecords) {
                String pk = pkNames.stream()
                        .map(p -> rec.getField(p) == null ? "__NULL__" : rec.getField(p).toString())
                        .collect(Collectors.joining("|"));
                dedupMap.put(pk, rec); // 后写覆盖前写，保留最新
            }
            int before = allRecords.size();
            allRecords = new ArrayList<>(dedupMap.values());
            log.info("[Compact] table={} dedup done, before={} after={}", tableKey, before, allRecords.size());
        }

        // ===== 决定要重写哪些文件 =====
        // 去重模式必须重写全表（否则旧文件里的重复行还在）
        // 仅小文件模式只重写小文件
        Set<DataFile> filesToRemove = new HashSet<>();
        if (needDedup) {
            for (FileScanTask task : allTasks) {
                filesToRemove.add(task.file());
            }
        } else {
            for (FileScanTask task : smallTasks) {
                filesToRemove.add(task.file());
            }
        }

        // ===== 写新合并文件 =====
        DataFile newDataFile = IceBergBatchOperationHandler.writeDataFile(table, allRecords, tableKey);

        // ===== 原子提交 =====
        try {
            RewriteFiles rewrite = table.newRewrite()
                    .rewriteFiles(filesToRemove, Collections.singleton(newDataFile));
            rewrite.commit();
            log.info("[Compact] table={} rewrite ok: {} files -> 1 file rows={} newSize={} bytes",
                    tableKey, filesToRemove.size(), allRecords.size(), newDataFile.fileSizeInBytes());
        } catch (org.apache.iceberg.exceptions.ValidationException ve) {
            log.warn("[Compact] table={} skip, concurrent conflict, retry next cycle. msg={}",
                    tableKey, ve.getMessage());
            return;
        }

        // ===== 过期旧快照 =====
        table.expireSnapshots()
                .expireOlderThan(System.currentTimeMillis() - 24L * 3600 * 1000)
                .retainLast(SNAPSHOTS_TO_RETAIN)
                .commit();
        log.info("[Compact] table={} expire snapshots done, retainLast={}", tableKey, SNAPSHOTS_TO_RETAIN);
    }
}