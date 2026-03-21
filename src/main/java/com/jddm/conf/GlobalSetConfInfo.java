package com.jddm.conf;
import com.jddm.common.Constant;
import com.jddm.vo.RowOperation;
import com.jddm.vo.SequencedPackage;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

import javax.activation.MimeTypeParameterList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class GlobalSetConfInfo {
    /**
     * key = "schema.table"，value = 该表的列名数组（全小写，按 schema 列顺序）
     * 避免热路径反复 toLowerCase() 和字符串拼接
     */
    public static Map<String, String[]> tableColumnNamesCache = new ConcurrentHashMap<>();

    /**
     * key = "schema.table"，value = 该表的类型转换器数组（按列位置索引）
     * 热路径直接 converters[colIdx].apply(rawValue)，消灭 instanceof 链
     */
    public static Map<String, java.util.function.Function<String, Object>[]> tableColumnConvertersCache = new ConcurrentHashMap<>();

    /**
     * key = "schema.table"，value = 列名 → 列位置 的快速查找 Map
     * 用于将 colName 转换为 GenericRecord.set(int pos) 的位置
     */
    public static Map<String, Map<String, Integer>> tableColumnPosCache = new ConcurrentHashMap<>();


    public static Map<String,Schema> IceBergSchemaCahceMap = new ConcurrentHashMap<>();
    public static Map<String,ImmutableList.Builder<GenericRecord>> IceBergSchemaImmuTableRecordMap = new ConcurrentHashMap<>();
    public static Map<String,ImmutableList.Builder<GenericRecord>> IceBergSchemaImmuTableDeleteRecordMap = new ConcurrentHashMap<>();
    public static Map<String,GenericRecord> IceBergTableGnericCacheMap = new ConcurrentHashMap<>();
    public static Map<String,Table> IceBergCacheTableMap = new ConcurrentHashMap<>();
    public static Map<String,Integer> jddmEngineTypeByNumberColMap = new ConcurrentHashMap<>();
    public static Map<String,String> IceBergTableCacheFileMap = new ConcurrentHashMap<>();
    public static Map<String,Boolean> IceBergOperationCompleteMap = new ConcurrentHashMap<String,Boolean>();
    public static Map<String,AtomicInteger> IceBergOperationBeginMap = new ConcurrentHashMap<String,AtomicInteger>();
    public static ArrayBlockingQueue<SequencedPackage> icebergEngineOperationQueue = new ArrayBlockingQueue(Constant.icebergOperationQueueSize);
    public static Map<String, List<String>> TablePkColCacheMap = new ConcurrentHashMap<>();
    // 在 importHiveTable_IceBerg_Table 建表时一次性填充，之后 O(1) 查询
    public static Map<String, org.apache.iceberg.types.Type> columnTypeCache = new ConcurrentHashMap<>();
    /**
     * key = db.table.threadId，value 用 LinkedBlockingDeque 支持 addFirst（用于失败回滚）：
     */
    public static Map<String, LinkedBlockingDeque<RowOperation>> IceBergSchemaImmuTableOpsMap = new ConcurrentHashMap<>();

    /**
     * 全量直写 Appender 上下文 Map，key = "schema.table.threadId"。
     * 每个 worker 线程独占一个 FullLoadDirectWriter 实例，天然线程隔离，无需加锁。
     */
    public static Map<String, FullLoadDirectWriter> fullLoadDirectWriterMap = new ConcurrentHashMap<>();

    /**
     * 全量直写上下文内部类：持有本线程当前正在写的本地 Parquet FileAppender 及行数统计。
     */
    public static class FullLoadDirectWriter {
        public final String tableKeyName;
        public final java.io.File localFile;
        public final org.apache.iceberg.io.OutputFile outputFile;
        public final org.apache.iceberg.io.FileAppender<org.apache.iceberg.data.GenericRecord> appender;
        public int rowCount = 0;
        /**
         * 标记该 writer 当前是否正在被 owner 线程写入。
         * 定时器线程看到 true 时跳过，不强行关闭，等下一个定时器周期再处理。
         * 使用 volatile 保证多线程可见性。
         */
        public volatile boolean activelyWriting = false;

        public FullLoadDirectWriter(String tableKeyName,
                                    java.io.File localFile,
                                    org.apache.iceberg.io.OutputFile outputFile,
                                    org.apache.iceberg.io.FileAppender<org.apache.iceberg.data.GenericRecord> appender) {
            this.tableKeyName = tableKeyName;
            this.localFile    = localFile;
            this.outputFile   = outputFile;
            this.appender     = appender;
        }
    }
}