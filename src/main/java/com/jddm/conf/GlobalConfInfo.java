package com.jddm.conf;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


public class GlobalConfInfo{

	//2023/1/13 3:17 PM; Auth:JH ; 增加缓存加载数据字典的计数Map
	public static Map<String,AtomicInteger> reloadTableVoCalcMap = new ConcurrentHashMap<String,AtomicInteger>();
	//2021-10-11 12:30 PM; Auth:JH ; 记录parquet模式下，根据Key【schemaName.tableName】值缓存最后DML数据时间；
	public static Map<String,Long> lastDataWriteTimerByParquetMap = new ConcurrentHashMap<String,Long>();
	public static Map<String,AtomicInteger> engineAtomicByTableKeyMap = new ConcurrentHashMap<String,AtomicInteger>();
	public static Map<Long,AtomicInteger>  txtFileNoSpliMap = new ConcurrentHashMap<Long,AtomicInteger>();
	public static Map<String,String> jddmEngineByHiveTableCacheMap = new ConcurrentHashMap<>();
	private static Configuration conf = null;
	public static Configuration getConf() {
		return conf;
	}
	public static void setConf(Configuration conf) {
		GlobalConfInfo.conf = conf;
	}
	



	
}
