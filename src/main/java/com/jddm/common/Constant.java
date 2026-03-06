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

	/**
	 * Log level switch loaded from config.properties ENGINE_LOG_LEVEL.
	 * true  = debug: print per-row colData, per-op merge trace, PK decision logs.
	 * false = info:  print batch-level summaries only (default, production).
	 */
	public static boolean debugLogEnabled = false;
}


