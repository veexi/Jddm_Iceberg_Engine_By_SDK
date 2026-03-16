package com.jddm.common;


import java.util.concurrent.atomic.AtomicBoolean;

public class Constant {

	/** Hive Metastore 通信地址 (Thrift 协议) */
	public static String hiveMetastoreUris="";
	/** 引擎自定义错误状态码，用于与外部监控系统通信 */
	public static volatile int customJddmEngineErrorFlag = 0;
	/** 引擎类型标识信息，显示在日志头部 */
	public static String JddmEngineTypeInfo="";
	/** SQLite 中 DDL 重新加载任务的缓存 Key */
	public static String reloadKeyName="reLoadeKey";

	/** 是否启用基于本地服务器时间的自动分区（主要用于无源表分区时的补齐） */
	public static boolean  hiveTablePartitionUsingLocalTimerFlag=false;
	/** 分区时间格式，如 yyyyMMdd */
	public static String  hiveTablePartitionLocalTimerFormatter="";
	/** 单批次累积记录数阈值，达到此值后将触发数据刷盘和 Iceberg 事务提交 */
	public static int writeCountNoToHiveFile=10000;
	/** HDFS 基础路径或 NameNode 地址，如 hdfs://nameservice1 */
	public static String fsDefaultInfo="";
	/** 默认目标数据库名称 */
	public static String settingDataBaseName="";
	/** Hive 表状态检查重试次数 */
	public static Integer hiveDiffTimers=5;
	/** 引擎程序当前的物理工作目录 */
	public static String basicWorkPath="";
	/** 引擎接收 DML 包的 Socket 服务端口 */
	public static String socketServerPort="8313";
	/** 刷盘批次的最大记录上限，防止单个事务过大导致内存溢出 */
	public static final int flushBatchMaxSize = 100000;

	/** Socket 服务的处理线程池并发数 */
	public static String socketThreadPoolSize="20";
	/** 引擎基础日志级别控制 */
	public static int LOG_AGENT_LEVEL=1000;
	/** 数据同步开关，通过 AtomicBoolean 确保多线程下的原子可见性 */
	public static final AtomicBoolean writeToIceBergDBFlag = new AtomicBoolean(true);
	/** 是否启用 Kafka 监控指标入库 */
	public static boolean kafkaMonitorToDBType=false;
	/** 本地服务器 IP 地址缓存 */
	public static String localHostIpAddress="";

	/** Iceberg mode: trajectory=append-only, transaction=CRUD+RowDelta */
	public static String icebergWriteMode = "trajectory";

	/** 小文件合并定时间隔（秒），默认 3600s = 1小时，由 COMPACT_INTERVAL_SECONDS 控制 */
	public static int compactIntervalSeconds = 3600;

	/** 小文件合并阈值（字节），默认 128MB，由 HIVE_FILE_CACHE_SIZE（单位 M）控制 */
	public static long compactSmallFileSizeBytes = 128L * 1024 * 1024;

	/**
	 * COW 去重模式，由 COW_MODE 控制：
	 * batch  = 每批次写入时实时去重
	 * timer  = 仅定时任务去重（写性能高，依赖 compactDedupEnabled=true）
	 * none   = 不去重（默认，纯 RowDelta 模式，避免大表重写性能隐患，需要查询引擎支持 equality delete）
	 */
	public static String cowMode = "none";

	/**
	 * 定时任务是否执行去重合并，由 COMPACT_DEDUP_ENABLED 控制。
	 * cowMode=timer 时建议开启，cowMode=batch 时此项无效。
	 * 默认 false。
	 */
	public static boolean compactDedupEnabled = false;

	/**
	 * 定时任务是否执行小文件合并，由 COMPACT_SMALL_FILES_ENABLED 控制。
	 * 默认 false（避免大表扫描合并性能隐患）。
	 */
	public static boolean compactSmallFilesEnabled = false;

	/**
	 * Log level switch loaded from config.properties ENGINE_LOG_LEVEL.
	 * true  = debug: print per-row colData, per-op merge trace, PK decision logs.
	 * false = info:  print batch-level summaries only (default, production).
	 */
	public static boolean debugLogEnabled = false;
	/** true = 启用 Kerberos；false = 普通认证（默认）。由 Engine_Authentication_Mode 控制 */
	public static boolean kerberosEnabled = false;
	public static String kerberosPrincipal = "";
	public static String kerberosKeytabPath = "";
	/** krb5.conf 文件路径，由 KERBEROS_KRB5_CONF_FILE 控制，不填则使用系统默认 */
	public static String krb5ConfFilePath = "";
	/** dfs.namenode.kerberos.principal，由 ZK_SECURITY_PRINCIPAL_INSTANCE 控制，不填则不设置 */
	public static String hdfsNamenodePrincipal = "";
	public static String hiveMetastorePrincipal = "";
	public static boolean dropTableFlag = false;
	public static int icebergOperationQueueSize = 10000;
}