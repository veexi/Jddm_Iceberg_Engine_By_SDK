package com.jddm.operation.timer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jddm.common.Constant;
import com.jddm.conf.GlobalSetConfInfo;
import com.jddm.utils.KerberosAuthUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 针对全量加载（Full Load）数据的异步提交器。
 * 采用手动序列化 Metrics 的方式，确保 ByteBuffer (Base64格式) 安全落地。
 */
public class IcebergFullLoadAsyncCommitter implements Runnable {
    private static final Logger log = LogManager.getLogger(IcebergFullLoadAsyncCommitter.class);
    private static final String CACHE_DIR_NAME = "FileCache";
    private static final ObjectMapper mapper = new ObjectMapper();

    public static final Map<String, FileSystem> fsCache = new ConcurrentHashMap<>();

    /**
     * 自定义元数据 POJO，用于完美保留 DataFile 的所有核心统计信息
     */
    public static class FullLoadMeta {
        public String fileName;
        public long recordCount;
        public long fileSize;
        public Map<Integer, Long> columnSizes;
        public Map<Integer, Long> valueCounts;
        public Map<Integer, Long> nullValueCounts;
        public Map<Integer, Long> nanValueCounts;
        public Map<Integer, String> lowerBounds; // Base64 编码
        public Map<Integer, String> upperBounds; // Base64 编码
        public Map<String, Object> partitionData; // 存储分区值
    }

    @Override
    public void run() {
        log.info("[AsyncCommitter] Starting full-load async committer thread...");
        while (true) {
            try {
                processCache();
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                log.warn("[AsyncCommitter] Thread interrupted, exiting...");
                break;
            } catch (Exception e) {
                log.error("[AsyncCommitter] Unexpected error in poll loop", e);
            }
        }
    }

    private void processCache() {
        File cacheRootDir = new File(Constant.basicWorkPath, CACHE_DIR_NAME);
        if (!cacheRootDir.exists() || !cacheRootDir.isDirectory()) return;

        File[] tableDirs = cacheRootDir.listFiles(File::isDirectory);
        if (tableDirs == null) return;

        for (File tableDir : tableDirs) {
            String tableKey = tableDir.getName();
            Table table = GlobalSetConfInfo.IceBergCacheTableMap.get(tableKey);
            if (table == null) continue;

            File[] metaFiles = tableDir.listFiles((dir, name) -> name.endsWith(".meta.json"));
            if (metaFiles == null) continue;

            for (File metaFile : metaFiles) {
                try {
                    handleSingleTask(table, tableKey, metaFile);
                } catch (Exception e) {
                    log.error("[AsyncCommitter] Failed to process task: {} for table: {}", metaFile.getName(), tableKey, e);
                }
            }
        }
    }

    private void handleSingleTask(Table table, String tableKey, File metaFile) throws Exception {
        String baseName = metaFile.getName().replace(".meta.json", "");
        File parquetFile = new File(metaFile.getParentFile(), baseName + ".parquet");

        if (!parquetFile.exists()) {
            metaFile.delete();
            return;
        }

        // 1. 反序列化 Meta
        FullLoadMeta meta = mapper.readValue(metaFile, FullLoadMeta.class);

        // 2. 准备 HDFS 路径
        String hdfsDestPath = table.location() + "/data/" + tableKey.replace(".", "/") + "/" + meta.fileName;

        // 3. 上传 HDFS
        FileSystem fs = getOrCreateFs(table, tableKey);
        Path src = new Path(parquetFile.getAbsolutePath());
        Path dst = new Path(hdfsDestPath);
        Path tmpDst = new Path(hdfsDestPath + ".tmp"); // 引入临时路径

        if (!fs.exists(dst)) {
            log.info("[AsyncCommitter] Uploading {} to HDFS: {}", tableKey, hdfsDestPath);
            // 1. 先传到临时文件
            fs.copyFromLocalFile(false, true, src, tmpDst);
            // 2. 原子的 rename 操作
            if (!fs.rename(tmpDst, dst)) {
                throw new RuntimeException("HDFS rename 失败: " + tmpDst + " -> " + dst);
            }
        }

        // 4. 还原 Metrics (将 Base64 还原为 ByteBuffer)
        Metrics metrics = new Metrics(
                meta.recordCount,
                meta.columnSizes,
                meta.valueCounts,
                meta.nullValueCounts,
                meta.nanValueCounts,
                decodeBounds(meta.lowerBounds),
                decodeBounds(meta.upperBounds)
        );

        // 5. 构造 DataFile 并提交
        DataFiles.Builder builder = DataFiles.builder(table.spec())
                .withPath(hdfsDestPath)
                .withFormat(org.apache.iceberg.FileFormat.PARQUET)
                .withFileSizeInBytes(meta.fileSize)
                .withMetrics(metrics);

        // 如果有分区信息，目前简单处理：如果是全量通常是覆盖，分区逻辑由 builder 自动根据路径/spec处理（或者手动填入）
        // 为简化，这里假设 table.spec().isPartitioned() 时，我们在 meta 里存了分区路径
        if (table.spec().isPartitioned() && meta.partitionData != null) {
            // 注意：这里需要根据具体的 PartitionKey 对象还原，为保持通用，建议使用 withPartitionPath 
            // 或者在 saveToLocalCache 时存下 partition path
        }

        table.newAppend().appendFile(builder.build()).commit();

        log.info("[AsyncCommitter] Commit ok, cleaning local: {}", baseName);
        metaFile.delete();
        parquetFile.delete();
    }
    private FileSystem getOrCreateFs(Table table, String tableKey) throws Exception {
        FileSystem fs = fsCache.get(tableKey);
        if (fs != null) return fs;

        // 直接从 table.io() 取已经正确初始化过的 Configuration
        // 这个 conf 是 Iceberg 引擎启动时就配好的，hdfs-site.xml/HA 全在里面
        Configuration conf;
        if (table.io() instanceof org.apache.iceberg.hadoop.HadoopFileIO) {
            conf = ((org.apache.iceberg.hadoop.HadoopFileIO) table.io()).conf();
            log.info("[AsyncCommitter] Reusing HadoopFileIO conf for table={}", tableKey);
        } else {
            // 兜底：走原来的工具类（kerberos 开启场景）
            conf = KerberosAuthUtil.buildHadoopConf();
            log.info("[AsyncCommitter] Fallback to KerberosAuthUtil conf for table={}", tableKey);
        }

        fs = FileSystem.get(new Path(table.location()).toUri(), conf);
        fsCache.put(tableKey, fs);
        log.info("[AsyncCommitter] FileSystem initialized and cached for table={}", tableKey);
        return fs;
    }
    public static void saveToLocalCache(String tableKey, DataFile dataFile, File localParquetFile) throws Exception {
        File cacheDir = new File(Constant.basicWorkPath, CACHE_DIR_NAME + File.separator + tableKey);
        if (!cacheDir.exists()) cacheDir.mkdirs();

        FullLoadMeta meta = new FullLoadMeta();
        meta.fileName = localParquetFile.getName();
        meta.recordCount = dataFile.recordCount();
        meta.fileSize = dataFile.fileSizeInBytes();
        meta.columnSizes = dataFile.columnSizes();
        meta.valueCounts = dataFile.valueCounts();
        meta.nullValueCounts = dataFile.nullValueCounts();
        meta.nanValueCounts = dataFile.nanValueCounts();
        meta.lowerBounds = encodeBounds(dataFile.lowerBounds());
        meta.upperBounds = encodeBounds(dataFile.upperBounds());

        File metaFile = new File(cacheDir, localParquetFile.getName().replace(".parquet", ".meta.json"));
        mapper.writeValue(metaFile, meta);

        log.info("[AsyncCommitter] Data cached locally with Metrics: {}/{}", tableKey, meta.fileName);
    }

    private static Map<Integer, String> encodeBounds(Map<Integer, ByteBuffer> bounds) {
        if (bounds == null) return null;
        Map<Integer, String> encoded = new HashMap<>();
        for (Map.Entry<Integer, ByteBuffer> entry : bounds.entrySet()) {
            ByteBuffer buffer = entry.getValue().duplicate();
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            encoded.put(entry.getKey(), Base64.getEncoder().encodeToString(bytes));
        }
        return encoded;
    }

    private static Map<Integer, ByteBuffer> decodeBounds(Map<Integer, String> encoded) {
        if (encoded == null) return null;
        Map<Integer, ByteBuffer> decoded = new HashMap<>();
        for (Map.Entry<Integer, String> entry : encoded.entrySet()) {
            byte[] bytes = Base64.getDecoder().decode(entry.getValue());
            decoded.put(entry.getKey(), ByteBuffer.wrap(bytes));
        }
        return decoded;
    }
}
