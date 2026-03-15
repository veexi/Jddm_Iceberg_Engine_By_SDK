package com.jddm.common;


import java.util.concurrent.atomic.AtomicBoolean;

public class Constant {

	public static String hiveMetastoreUris="";
	public static volatile int customJddmEngineErrorFlag = 0;
	public static String JddmEngineTypeInfo="";
	public static String reloadKeyName="reLoadeKey";
	
	//2023/1/4 3:59 PM; Auth:JH 增加hive是否
	public static boolean  hiveTablePartitionUsingLocalTimerFlag=false;
	public static String  hiveTablePartitionLocalTimerFormatter="";
	public static int writeCountNoToHiveFile=10000;
	public static String fsDefaultInfo="";
	public static String settingDataBaseName="";
	//2023/9/19 16:18 PM; Auth:JH;
	public static Integer hiveDiffTimers=5;
	public static String basicWorkPath="";
	public static String socketServerPort="8313";
	public static String socketThreadPoolSize="20";
	public static int LOG_AGENT_LEVEL=1000;
    public static final AtomicBoolean writeToIceBergDBFlag = new AtomicBoolean(true);
    public static boolean kafkaMonitorToDBType=false;
	public static String localHostIpAddress="";

	/** Iceberg mode: trajectory=append-only, transaction=CRUD+RowDelta */
	public static String icebergWriteMode = "trajectory";

	/** 小文件合并定时间隔（秒），默认 3600s = 1小时，由 HIVE_FILE_MODIFY_TIMES 控制 */
	public static int compactIntervalSeconds = 3600;

	/** 小文件合并阈值（字节），默认 128MB，由 HIVE_FILE_CACHE_SIZE（单位 M）控制 */
	public static long compactSmallFileSizeBytes = 128L * 1024 * 1024;

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
}


