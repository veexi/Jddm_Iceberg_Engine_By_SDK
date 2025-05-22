package com.jddm.common;


public class Constant {
	public static boolean writeToIceBergDBFlag=false;
	public static String hiveMetastoreUris="";
	public static volatile int customJddmEngineErrorFlag = 0;
	public static String JddmEngineTypeInfo="";
	public static String reloadKeyName="reLoadeKey";
	
	//2023/1/4 3:59 PM; Auth:JH 增加hive是否
	public static boolean  hiveTablePartitionUsingLocalTimerFlag=false;
	public static String  hiveTablePartitionLocalTimerFormatter="";
	public static int writeCountNoToHiveFile=30;
	public static String fsDefaultInfo="";
	public static String settingDataBaseName="";
	//2023/9/19 16:18 PM; Auth:JH;
	public static Integer hiveDiffTimers=60;
	public static String basicWorkPath="";
	public static String socketServerPort="8313";
	public static String socketThreadPoolSize="20";

}


