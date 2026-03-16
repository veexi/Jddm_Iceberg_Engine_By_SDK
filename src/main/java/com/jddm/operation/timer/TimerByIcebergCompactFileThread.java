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
 * 定时维护任务：扫描所有注册的 Iceberg 表，根据配置执行“数据去重合并”和“小文件合并”操作，并清理过期快照。
 *
 * 核心配置项（来自 config.properties）：
 *   COMPACT_DEDUP_ENABLED=true/false       是否启用数据去重（主要用于 cowMode=timer 模式）
 *   COMPACT_SMALL_FILES_ENABLED=true/false 是否启用小文件合并（优化查询性能）
 *   COMPACT_INTERVAL_SECONDS=3600          维护任务触发间隔（秒）
 *   HIVE_FILE_CACHE_SIZE=128               小文件定义阈值（单位：MB）
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

        // 第一阶段：扫描表元数据，识别并筛选出需要处理的数据文件
        List<FileScanTask> allTasks = new ArrayList<>();   // 用于去重模式的全量文件任务
        List<FileScanTask> smallTasks = new ArrayList<>(); // 用于合并模式的小文件任务

        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                allTasks.add(task);
                // 筛选出小于配置阈值的文件
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

        // 第二阶段：读取全表当前有效数据
        // 注意：IcebergGenerics.read 会自动应用 Delete File，因此读出的数据是业务层面的“最新状态”列表。
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

        // 第三阶段：执行数据去重逻辑
        if (needDedup) {
            LinkedHashMap<String, GenericRecord> dedupMap = new LinkedHashMap<>();
            for (GenericRecord rec : allRecords) {
                // 基于主键构建唯一 Key，用于去重
                String pk = pkNames.stream()
                        .map(p -> rec.getField(p) == null ? "__NULL__" : rec.getField(p).toString())
                        .collect(Collectors.joining("|"));
                // 采用“后写覆盖”策略：在遍历过程中最后出现的记录将作为最终生效记录
                dedupMap.put(pk, rec); 
            }
            int before = allRecords.size();
            allRecords = new ArrayList<>(dedupMap.values());
            log.info("[Compact] table={} dedup done, before={} after={}", tableKey, before, allRecords.size());
        }

        // 第四阶段：确定待移除的旧文件范围，并写出合并后的新 Data File
        // 1. 确定物理策略：去重模式下必须重写全表文件以确保移除隐式重复行；仅小文件合并模式则只重写选中的小文件。
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

        // 2. 将全量（或合并后）的数据写入一个新的物理文件
        DataFile newDataFile = IceBergBatchOperationHandler.writeDataFile(table, allRecords, tableKey);

        // 第五阶段：执行原子替换操作，完成元数据更新
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

        // 第六阶段：清理过期快照，释放底层文件存储空间
        table.expireSnapshots()
                .expireOlderThan(System.currentTimeMillis() - 24L * 3600 * 1000) // 默认过期 24 小时前的快照
                .retainLast(SNAPSHOTS_TO_RETAIN)
                .commit();
        log.info("[Compact] table={} expire snapshots done, retainLast={}", tableKey, SNAPSHOTS_TO_RETAIN);
    }
}