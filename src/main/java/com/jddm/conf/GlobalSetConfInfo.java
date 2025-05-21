package com.jddm.conf;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class GlobalSetConfInfo {
	
	public static Map<String,Schema> IceBergSchemaCahceMap = new ConcurrentHashMap<>();
	public static Map<String,ImmutableList.Builder<GenericRecord>> IceBergSchemaImmuTableRecordMap = new ConcurrentHashMap<>();
	public static Map<String,ImmutableList.Builder<GenericRecord>> IceBergSchemaImmuTableDeleteRecordMap = new ConcurrentHashMap<>();
	public static Map<String,GenericRecord> IceBergTableGnericCacheMap = new ConcurrentHashMap<>();
	public static Map<String,Table> IceBergCacheTableMap = new ConcurrentHashMap<>();
	public static Map<String,Integer> jddmEngineTypeByNumberColMap = new ConcurrentHashMap<>();
	public static Map<String,String> IceBergTableCacheFileMap = new ConcurrentHashMap<>();
	public static Map<String,Boolean> IceBergOperationCompleteMap = new ConcurrentHashMap<String,Boolean>();
	public static Map<String,AtomicInteger> IceBergOperationBeginMap = new ConcurrentHashMap<String,AtomicInteger>();
	public static ArrayBlockingQueue<Object> icebergEngineOperationQueue = new ArrayBlockingQueue(320000);





}
