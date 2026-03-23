package com.jddm.boot;

import com.dsg.analysis.tableInfo.vo.SourceTableInfoVo;
import com.dsg.analysis.tableInfo.vo.TableInfoVo;

import com.dsg.analysis.vo.PackageReturnVo;
import com.jddm.vo.SequencedPackage;
import com.dsg.operation.common.ConstantSet;
import com.jddm.common.Constant;
import com.jddm.conf.Configuration;
import com.jddm.conf.GlobalConfInfo;
import com.jddm.conf.GlobalSetConfInfo;
import com.jddm.conf.InitConfigParameter;
import com.jddm.exception.InitializationException;
import com.jddm.killOper.JddmEngineKillHandler;
import com.jddm.operation.ddl.IceBergTableOperationByEngine;
import com.jddm.operation.timer.ReLoaderTableInfoBySqliteDB;
import com.jddm.operation.timer.TimerByHiveCacheFileThreadV1;
import com.jddm.thread.OperationTotalSyncByIceBergThreadPool;
import com.jddm.utils.KerberosAuthUtil;
import com.publics.cache.TableAllCacheInfo;
import com.publics.common.ConstantFileInfoSet;
import com.publics.common.ConstantPubSet;
import com.publics.common.ConstantPublic;
import com.publics.conf.GlobalConfCommInfo;
import com.publics.engine.operation.socketSecGeneration.SocketGeneralEngine;
import com.publics.engine.state.EngineStateInfo;
import com.publics.operation.jdbcOper.embeddedDB.SQLiteJDBC;
import com.publics.operation.socket.ExecSQLInfoVo;
import com.publics.utils.EngineLoadJarPkg;
import com.publics.utils.FileUtils;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.*;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.jar.Manifest;

import static com.publics.utils.PublicOperationUtils.findAllJarFileList;

/**
 * 引擎启动类：负责整个 Jddm Iceberg 引擎的生命周期管理，包括环境初始化、配置加载、模块动态载入以及核心服务（如 Socket 监听和线程池）的启动。
 */
public class StartIcebergEngine {
    private final static String CONFIG_INFO_PATH ="config/config.properties";
    private final static String LOG_XML_PATH = "config/log4jConfig.xml";
    private static  String hiveFilePath = "hdfsFile";
    private static int totalSyncNO=1;
    public static final String version = StartIcebergEngine.class.getPackage().getImplementationVersion();


    /**
     * 引擎主入口：负责加载本地配置、设置系统路径、初始化 Catalog 驱动并启动数据同步主引擎。
     */
    public static void main(String[] args) throws Exception {
        Logger log = LogManager.getLogger(StartIcebergEngine.class);
        Properties properties = new Properties();
        try {
            String ip="";
            if(ip.equals("127.0.0.1")){
                Constant.localHostIpAddress=getThisEngineLocalIp();
            } else {
                Constant.localHostIpAddress=ip;
            }

            Properties props = System.getProperties(); //系统属性
            Constant.basicWorkPath = props.getProperty("user.dir");
            ConstantSet.baseWorkDir = Constant.basicWorkPath;
            ConstantPubSet.baseWorkDir = Constant.basicWorkPath;

            Configuration conf = new Configuration(CONFIG_INFO_PATH);
            GlobalConfInfo.setConf(conf);

            InitConfigParameter initConfigParameter = new InitConfigParameter();
            if (initConfigParameter.initConf()){
                initConfigParameter.printParameters();
            }else {
                throw new Exception("Config parameters initialization failed, please check!");
            }

            printAsciiVerison(initializeServices());
            File libJarfile = new File(ConstantSet.baseWorkDir+File.separator+ ConstantFileInfoSet.cacheFileInfoSet.ModuleLibInfo.getPath());
            List<String> allJarList= new ArrayList<String>();
            log.info(Constant.JddmEngineTypeInfo+" ###### General Main Engine Begin To Loading Module ... ... ");
            findAllJarFileList(libJarfile,allJarList);
            String dataBaseConfXmlConfiger="";
            String dynamicLoaderJarFileName ="";
            URL dataSyncUrl = null;
            URLClassLoader classLoader = null;

            Class<?> syncInitInfoCls = null;
            for(String jarFileName:allJarList) {

                log.info(Constant.JddmEngineTypeInfo+" Main Engine user.dir ::"+ConstantSet.baseWorkDir+" ipAddress ::"+Constant.localHostIpAddress+" size :"+allJarList.size()+" jarName ::"+jarFileName);
                switch(jarFileName) {

                    case "jddm.engine.module.databaseOperation.jar":  //缓存数据入库模块
						/*dataBaseConfXmlConfiger = "config/module/dataBase/applicationContext.xml";
						GlobalDBConfInfo.setContext(new FileSystemXmlApplicationContext(dataBaseConfXmlConfiger));
						log.info(" ");
						log.info(" ##################################################################################################");
						log.info("  Please wait a moment, Connect to Setting Mysql DataBase ......");
						MysqlDataBaseOperation mysqlDataBaseOperation = new MysqlDataBaseOperation();
						mysqlDataBaseOperation.queryMysqlDataBaseBaseInfo();
						if(mysqlDataBaseOperation.existInDataBaseVerification("dataxone_bigdatas_db")){

							mysqlDataBaseOperation.verificationTableNameExistInMysqlDB("dataxone_bigdatas_db","fl_kafka_topic_send_info");
							mysqlDataBaseOperation.verificationTableNameExistInMysqlDB("dataxone_bigdatas_db","fl_kafka_partition_send_info");
							mysqlDataBaseOperation.verificationTableNameExistInMysqlDB("dataxone_bigdatas_db","real_kafka_partition_send_info");

						}
						log.info(" ##################################################################################################");
						log.info(" ");*/

                        dynamicLoaderJarFileName = ConstantSet.baseWorkDir+File.separator+"module"+File.separator+"jddm.engine.module.databaseOperation.jar";

                        EngineLoadJarPkg.loadJar(dynamicLoaderJarFileName);

                        log.info(ConstantSet.jddmEngine.MYSQL.getEngineName()+"Loding DataBase Operation jar Path ::"+dynamicLoaderJarFileName);
                        //从容器中获取Class对象
                        dataSyncUrl = new File(dynamicLoaderJarFileName).toURI().toURL();
                        classLoader = new URLClassLoader(new URL[] { dataSyncUrl }, Thread.currentThread().getContextClassLoader());

                        syncInitInfoCls = classLoader.loadClass("com.jddm.engine.database.OperationByJddmDataBase");
                        if (syncInitInfoCls == null) {
                            return;
                        }
                        log.info(ConstantSet.jddmEngine.MYSQL.getEngineName()+"DataBase Config --------> workDirPath ::"+ConstantSet.baseWorkDir);
                        //dataBaseConfXmlConfiger = projectRunningPath+File.separator+"config/module/dataBase/applicationContext.xml";
                        dataBaseConfXmlConfiger = "config/module/dataBase/applicationContext.xml";
                        log.info(" Jddm Engine[Mysql] DataBase Config --------> workDirPath ::"+dataBaseConfXmlConfiger);
                        //初始化数据同步配置文件
                        Method cacheMethod = syncInitInfoCls.getDeclaredMethod("initDataBaseConfig", String.class);

                        cacheMethod.invoke(syncInitInfoCls, dataBaseConfXmlConfiger);

                        Constant.kafkaMonitorToDBType = true;

                        Object beanObj=EngineLoadJarPkg.searchBeanMethod("mysqldbOperationService");//从容器中获取Bean对象，并输出所有定义的方法
                        GlobalConfInfo.setDbOperServiceObj(beanObj);
                        log.info(Constant.JddmEngineTypeInfo+"Loding DataBase Operation jar Path ::"+dynamicLoaderJarFileName);
                        break;
                }

            }
            properties.setProperty("ServerPort", Constant.socketServerPort);
            properties.setProperty("ThreadPoolSize", Constant.socketThreadPoolSize);
            properties.setProperty("setQueueSize","1000");
            SocketGeneralEngine engine = SocketGeneralEngine.create("path")
                    .setting(properties)
                    .notifying((item) -> {
                        //DataVo dataVo = (DataVo) item;

                        switch (item.getClass().getSimpleName()) {
                            case "ExecSQLInfoVo":  //if(item instanceof ExecSQLInfoVo)
                                System.out.println(" EXEC_SQL :: " + item.getClass().getSimpleName() + " --->" + new String(((ExecSQLInfoVo) item).getExecSqlArr()));
                                break;
                            case "SourceTableInfoVo": // } else if(item instanceof SourceTableInfoVo) {
                                System.out.println(" Source_DDL ::" + ((SourceTableInfoVo) item).getOwner() + "." + ((SourceTableInfoVo) item).getTableName() + " colSize ::" + ((SourceTableInfoVo) item).getColumnList().size());
                                try {
                                    TableAllCacheInfo tableCacheInfo = new TableAllCacheInfo();
                                    SQLiteJDBC sqLiteJDBC= null;
                                    byte[] content = new byte[0x12];
                                    String cachekeyName = ((SourceTableInfoVo) item).getOwner().toLowerCase()+"."+((SourceTableInfoVo) item).getTableName().toLowerCase();
                                    GlobalConfCommInfo.cacheSourceTableInfoMap.put(cachekeyName, toTableInfo((SourceTableInfoVo) item));
                                    //tableCacheInfo.mergeSourceAndYloaderDictionary(cachekeyName,tableInfoVo);
//                                    tableCacheInfo.setSourceColumeTypeToYloaderDictionary(cachekeyName, ((TableInfoVo) item));
                                    //写入到sqlite 嵌入式数据库中
                                    sqLiteJDBC = new SQLiteJDBC();
                                    sqLiteJDBC.recordJddmCacheTable_ToSqliteDB(cachekeyName+"_source",((SourceTableInfoVo) item).getObjn()+"",content,tableCacheInfo.serializableTableVo_ToByteArray(toTableInfo((SourceTableInfoVo) item)));

                                    System.out.println(" Jddm Engine Plug-in  Source Table DDL schema&table    ::" + ((SourceTableInfoVo) item).getOwner() + "." + ((SourceTableInfoVo) item).getTableName());
                                    System.out.println(" Jddm Engine Plug-in  Source Table DDL ColumnSize   ::" + ((SourceTableInfoVo) item).getColumnList().size());
                                } catch (Exception e) {
                                    e.printStackTrace();

                                }
                                break;
                            case "TableInfoVo": //} else if(item instanceof TableInfoVo) {
                                System.out.println(" DDL ::" + ((TableInfoVo) item).getOwner() + "." + ((TableInfoVo) item).getTableName() + " colSize ::" + ((TableInfoVo) item).getColumnList().size());
                                Map<String, String> tableColumnMap = new LinkedHashMap<String, String>();
                                TableAllCacheInfo tableCacheInfo = new TableAllCacheInfo();
                                SQLiteJDBC sqLiteJDBC= null;
                                byte[] content = new byte[0x12];
                                String cachekeyName = ((TableInfoVo) item).getOwner().toLowerCase()+"."+((TableInfoVo) item).getTableName().toLowerCase();
                                if(GlobalConfCommInfo.cacheSourceTableInfoMap.containsKey(cachekeyName)) {
                                    //tableCacheInfo.mergeSourceAndYloaderDictionary(cachekeyName,tableInfoVo);
                                    tableCacheInfo.setSourceColumeTypeToYloaderDictionary(cachekeyName, ((TableInfoVo) item));
                                    //写入到sqlite 嵌入式数据库中
                                    sqLiteJDBC = new SQLiteJDBC();
                                    try {
                                        sqLiteJDBC.recordJddmCacheTable_ToSqliteDB(cachekeyName,((TableInfoVo) item).getObjn()+"",content,tableCacheInfo.serializableTableVo_ToByteArray((TableInfoVo) item));
                                    } catch (Exception e) {
                                        log.error("Jddm Engine Plug-in Table DDL Write To Sqlite Exception ! table={} objn={} err={}",
                                        cachekeyName, ((TableInfoVo) item).getObjn(), e.getMessage(), e);
                                    }

                                }
                                GlobalConfCommInfo.cacheSourceTableInfoMap.remove(cachekeyName);
                                if (!GlobalConfCommInfo.cacheTableInfoMap.containsKey(cachekeyName)){
                                    IceBergTableOperationByEngine iceBergTableOperationByEngine = new IceBergTableOperationByEngine();
                                    iceBergTableOperationByEngine.importHiveTable_IceBerg_Table((TableInfoVo) item, tableColumnMap);
                                }
                                GlobalConfCommInfo.cacheTableInfoMap.put(cachekeyName, (TableInfoVo) item);

                                break;
                            case "PackageReturnVo":
                                if(!ConstantPublic.jddmEngineStatFlag) {  // 获取 Signal 信号值，如果程序被 kill -9 之外的任意停止命令终止，在返回异常；

                                    switch(Constant.customJddmEngineErrorFlag) {

                                        case 10004:
                                            log.error(" JddmEngine(Java program) execution Stop Command !!! ");
                                            //socketReturnVo.setTradeType("kafkaData");
                                            break;
                                        case 10001: // hive Engine Exception ->   org.apache.hadoop.hive.ql.metadata.HiveException: Access denied: Unable to move source file /xxxx/xxxx/xxxxx
                                            log.error(ConstantPublic.jddmEngineError_Msg);
                                            //socketReturnVo.setTradeType("kafkaData");
                                            break;
                                        default:
                                            log.error(" unKnow Error !!! Please contact system engineer !");
                                            //socketReturnVo.setTradeType("kafkaData");
                                            break;
                                    }

                                }else {

                                }
                                System.out.println(" DML ::" + ((PackageReturnVo) item).getOwnerName() + "." + ((PackageReturnVo) item).getTableName() + " fileNo ::" + ((PackageReturnVo) item).getFileNo() + " Rows ::" + ((PackageReturnVo) item).getRowsCount());
                                try {
                                    // 核心修复：单线程接收端立即分配序列号，消除多线程分配时的竞态乱序
                                    long pktSeq = GlobalConfInfo.icebergEngineOperationSeq.getAndIncrement();
                                    putWithMemoryGuard(
                                            new SequencedPackage((PackageReturnVo) item, pktSeq));
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                                break;
                            default:
                                System.out.println(" OtherName ::" + item.getClass().getSimpleName() + " ---> " + item.toString());
                                break;
                        }

                    })
                    .build();
            engine.run();
            EngineStateInfo.writeEngineStateForJddm("start", "sucess");
        }catch (Exception exc){
            exc.printStackTrace();
            EngineStateInfo.writeEngineStateForJddm("exception", exc.getMessage());
        }


    }
    /**
     * 服务初始化：配置核心业务服务，包括 Kerberos 认证、元数据预加载、信号处理器注册以及各类任务调度线程池。
     */
private static boolean initializeServices() throws InitializationException {
    boolean returnFlag = false;
    ExecutorService fixedThreadPool = null;
    ScheduledExecutorService scheduledThreadPool = null;
    ReLoaderTableInfoBySqliteDB reLoaderTableInfoBySqliteDB = null;

    try {
        KerberosAuthUtil.loginFromKeytab();
        preloadTableDictFromSqlite();

        JddmEngineKillHandler jddmKillHandler = new JddmEngineKillHandler(5, TimeUnit.SECONDS);
        jddmKillHandler.registerSignal("TERM");
        jddmKillHandler.registerSignal("INT");

        scheduledThreadPool = Executors.newScheduledThreadPool(6);

        scheduledThreadPool.scheduleAtFixedRate(
                new TimerByHiveCacheFileThreadV1(), 5, 5, TimeUnit.SECONDS);

        GlobalConfInfo.reloadTableVoCalcMap.put(Constant.reloadKeyName, new AtomicInteger(0));
        scheduledThreadPool.scheduleAtFixedRate(
                new ReLoaderTableInfoBySqliteDB(), 5, 5, TimeUnit.SECONDS);

 /*       scheduledThreadPool.scheduleAtFixedRate(
                new TimerByIcebergCompactFileThread(), 5,
                Constant.compactIntervalSeconds, TimeUnit.SECONDS);*/

        scheduledThreadPool.scheduleAtFixedRate(
                KerberosAuthUtil::renewTgtIfNeeded, 1, 1, TimeUnit.HOURS);

        // 启动全量数据异步提交线程
        Executors.newSingleThreadExecutor().execute(new com.jddm.operation.timer.IcebergFullLoadAsyncCommitter());

        fixedThreadPool = Executors.newFixedThreadPool(50);
        for (int i = 0; i < totalSyncNO; i++) {
            fixedThreadPool.execute(new OperationTotalSyncByIceBergThreadPool());
        }
        returnFlag = true;

    } catch (Exception e) {
        returnFlag = false;
        throw new InitializationException("Service initialization failed", e);
    }
    return returnFlag;
}
/*    private static boolean initializeServices() throws InitializationException {
        boolean returnFlag = false;
        ExecutorService fixedThreadPool = null;
        ScheduledExecutorService scheduledThreadPool = null;
        ReLoaderTableInfoBySqliteDB reLoaderTableInfoBySqliteDB = null;

        try {
            preloadTableDictFromSqlite();

            JddmEngineKillHandler jddmKillHandler = new JddmEngineKillHandler(5, TimeUnit.SECONDS);
            jddmKillHandler.registerSignal("TERM");
            jddmKillHandler.registerSignal("INT");

            scheduledThreadPool = Executors.newScheduledThreadPool(5);
            TimerByHiveCacheFileThreadV1 timerByHiveCacheFileThread = new TimerByHiveCacheFileThreadV1();
            scheduledThreadPool.scheduleAtFixedRate(timerByHiveCacheFileThread, 5, 30, TimeUnit.SECONDS);

            scheduledThreadPool = Executors.newScheduledThreadPool(3);
            //设置reloaded DDL的计算器
            GlobalConfInfo.reloadTableVoCalcMap.put(Constant.reloadKeyName, new AtomicInteger(0));
            reLoaderTableInfoBySqliteDB = new ReLoaderTableInfoBySqliteDB();
            scheduledThreadPool.scheduleAtFixedRate(reLoaderTableInfoBySqliteDB, 5, 5, TimeUnit.SECONDS);

            fixedThreadPool = Executors.newFixedThreadPool(50);

            OperationTotalSyncByIceBergThreadPool operationTotalSyncByIceBergThreadPool = null;
            for (int i = 0; i < totalSyncNO; i++) {
                operationTotalSyncByIceBergThreadPool = new OperationTotalSyncByIceBergThreadPool();
                fixedThreadPool.execute(operationTotalSyncByIceBergThreadPool);
            }
            returnFlag=true;


        } catch (Exception e) {
            returnFlag=false;
            throw new InitializationException("Service initialization failed", e);

        }
        return returnFlag;
    }*/

    /**
     * SQLite 元数据预加载：在引擎启动初期，主动从本地 SQLite 数据库中恢复表字典信息。
     * 这样做能够确保在引擎重启后，即使 DDL 信号尚未到达，DML 数据包也能根据已有的元数据立即开始处理，提高系统可用性。
     */
    private static void preloadTableDictFromSqlite() {
        Logger log = LogManager.getLogger(StartIcebergEngine.class);
        try {
            SQLiteJDBC sqliteDB = new SQLiteJDBC();
            List<String> allKeys = sqliteDB.queryJddmCacheTableList();
            if (allKeys == null || allKeys.isEmpty()) {
                log.info("[Preload] SQLite no table cache, skip");
                return;
            }
            Set<String> tableKeys = new LinkedHashSet<>();
            for (String key : allKeys) {
                if (key != null && !key.endsWith("_source")) {
                    tableKeys.add(key.toLowerCase());
                }
            }
            if (tableKeys.isEmpty()) {
                log.info("[Preload] no valid table key, skip");
                return;
            }
            log.info("[Preload] start load {} tables from SQLite", tableKeys.size());
            TableAllCacheInfo tableCacheInfo = new TableAllCacheInfo();
            IceBergTableOperationByEngine iceBergTableOperationByEngine = new IceBergTableOperationByEngine();
            Map<String, String> tableColumnMap = new LinkedHashMap<>();
            int loaded = 0;
            for (String key : tableKeys) {
                try {
                    byte[] tableInfoArr = sqliteDB.query_TableContentObject_ByKey(key);
                    if (tableInfoArr == null || tableInfoArr.length < 4) {
                        log.error("[Preload] table {} no valid data, skip", key);
                        continue;
                    }
                    TableInfoVo tableInfoDBVo = tableCacheInfo.serializableTableVo_ToByteArray(tableInfoArr);
                    byte[] sourceTableInfoArr = sqliteDB.query_TableContentObject_ByKey(key + "_source");
                    if (sourceTableInfoArr != null && sourceTableInfoArr.length > 3) {
                        TableInfoVo sourceTableInfoDBVo = tableCacheInfo.serializableTableVo_ToByteArray(sourceTableInfoArr);
                        GlobalConfCommInfo.cacheSourceTableInfoMap.put(key, sourceTableInfoDBVo);
                        tableCacheInfo.mergeSourceAndYloaderDictionary(key, tableInfoDBVo);
                        GlobalConfCommInfo.cacheSourceTableInfoMap.remove(key);
                    }
                    GlobalConfCommInfo.cacheTableInfoMap.put(key, tableInfoDBVo);

                    GlobalConfInfo.jddmEngineByHiveTableCacheMap.put(key,
                                tableInfoDBVo.getOwner().toLowerCase() + "." + FileUtils.createTableName_ByJddmEngine(
                                        tableInfoDBVo.getOwner(), tableInfoDBVo.getTableName()));
                    if (!tableInfoDBVo.getColumnList().isEmpty()) {
                        iceBergTableOperationByEngine.importHiveTable_IceBerg_Table(tableInfoDBVo, tableColumnMap);
                    }
                    loaded++;
                    log.info("[Preload] loaded table {}", key);
                } catch (Exception e) {
                    log.error("[Preload] load table {} failed: {}", key, e.getMessage());
                    e.printStackTrace();
                }
            }
            log.info("[Preload] done, loaded {} tables", loaded);
        } catch (Exception e) {
            log.error("[Preload] SQLite preload failed: {}", e.getMessage());
            e.printStackTrace();
        }
    }

    public static void printAsciiVerison(boolean initializeFlag){
        String logLevel = "INFO";
        Logger log = LogManager.getLogger(StartIcebergEngine.class);
        System.out.format("%s\n", " ");
        System.out.format("%s\n", "                    _     _                ");
        System.out.format("%s\n", "   __ _ _          | |   | |         __ _ _");
        System.out.format("%s\n", "  / / / /    (_) __| | __| |__ _ __  \\ \\ \\ \\");
        System.out.format("%s\n", " / / / /     | |/ _' |/ _' | _   _ |  \\ \\ \\ \\");
        System.out.format("%s\n", "( ( ( (      | | (_| | (_| | || || |   ) ) ) )");
        System.out.format("%s\n", " \\ \\ \\ \\   __, '\\.__/'\\.__/'_||_||_|  / / / /");
        System.out.format("%s\n", "  \\_\\_\\_\\ |___/===================== /_/_/_/");
        System.out.format("%s\n", "    :: Dsg(java)Data EngineModuel ::      (v"+version+")");
        System.out.format("%s\n", " ");
        System.out.println("\t 32&64 bit (PROD), build#1,"+getBuildTimeString());
        log.info(" ");

        log.info("Jddm Iceberg Engine :: ("+version+")");

        String currentDir = System.getProperty("user.dir");
        String parentDir = new File(currentDir).getParent();
        if (Constant.LOG_AGENT_LEVEL==2000){
            logLevel = "DEBUG";
        }
        log.info("Build Time :: " + getBuildTimeString());
        log.info("ICEBERG_ENGINE WORKING DIR :: " + parentDir);
        log.info("LOG LEVEL :: " + logLevel);

        if (initializeFlag){
            log.info("Initializing Jddm Iceberg Engine Successfully.");
        }else {
            log.error("Initializing Jddm Iceberg Engine Failed.");
        }
        log.info("Iceberg version :: "+ org.apache.iceberg.Table.class.getPackage().getImplementationVersion());
        log.info("Validating engine connection to Iceberg source...");
/*        IcebergValidator icebergValidator = new IcebergValidator();
        try {
            icebergValidator.validateTableOperations();
        }catch (Exception e){
            throw new RuntimeException("Iceberg connection validate failed",e);
        }finally {
            icebergValidator=null;
        }*/

        log.info("Jddm Iceberg Engine started successfully.");
        log.info("...");
    }

    /**
     * 获取程序的构建时间信息。
     * 逻辑：通过读取 jar 包内的 MANIFEST.MF 元数据获取 Build-Timestamp，并转换为北京时间（+08:00）显示。
     */
    public static String getBuildTimeString(){
        String returnTime = "N/A";
        try (InputStream in =
                     StartIcebergEngine.class.getClassLoader()
                             .getResourceAsStream("META-INF/MANIFEST.MF")) {
            if (in == null) {
                return returnTime;
            }
            Manifest mf = new Manifest(in);
            String timestamp = mf.getMainAttributes()
                    .getValue("Build-Timestamp");
            if (timestamp == null || timestamp.isEmpty()) {
                return returnTime;
            }
            OffsetDateTime utc = OffsetDateTime.parse(timestamp);
            // 2. 改变偏移量到 +08:00（同一时刻）
            OffsetDateTime beijing = utc.withOffsetSameInstant(ZoneOffset.ofHours(8));
            returnTime = beijing.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        } catch (Exception e) {
            // Log or ignore instead of throwing RuntimeException to prevent startup failure
            returnTime = "N/A";
        }
        return returnTime;
    }
    public static int getTotalSyncNO() {
        return totalSyncNO;
    }

    public static void setTotalSyncNO(int totalSyncNO) {
        StartIcebergEngine.totalSyncNO = totalSyncNO;
    }

    public static String getHiveFilePath() {
        return hiveFilePath;
    }

    public static void setHiveFilePath(String hiveFilePath) {
        StartIcebergEngine.hiveFilePath = hiveFilePath;
    }
    public static TableInfoVo toTableInfo(SourceTableInfoVo src) {
        TableInfoVo dest = new TableInfoVo();
        try {
            // 参数顺序：dest, orig
            BeanUtils.copyProperties(dest, src);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return dest;
    }
    public static String getThisEngineLocalIp() {
        String localip = null;// 本地IP，如果没有配置外网IP则返回它
        String netip = null;// 外网IP
        try {
            Enumeration netInterfaces = NetworkInterface.getNetworkInterfaces();
            InetAddress ip = null;
            boolean finded = false;// 是否找到外网IP
            while (netInterfaces.hasMoreElements() && !finded) {
                NetworkInterface ni = (NetworkInterface) netInterfaces.nextElement();
                Enumeration address = ni.getInetAddresses();
                while (address.hasMoreElements()) {
                    ip = (InetAddress) address.nextElement();
                    if (!ip.isSiteLocalAddress() && !ip.isLoopbackAddress() && ip.getHostAddress().indexOf(":") == -1) {// 外网IP
                        netip = ip.getHostAddress();
                        finded = true;
                        break;
                    } else if (ip.isSiteLocalAddress() && !ip.isLoopbackAddress() && ip.getHostAddress().indexOf(":") == -1) {// 内网IP
                        localip = ip.getHostAddress();
                    }
                }
            }
        } catch (SocketException e) {
            e.printStackTrace();
        }

        if (netip != null && !"".equals(netip)) {
            return netip;
        } else {
            return localip;
        }
    }
    private static void putWithMemoryGuard(SequencedPackage item) throws InterruptedException {
        Runtime rt = Runtime.getRuntime();
//            long blockStart = 0;

            long maxMemory   = rt.maxMemory();
            long totalMemory = rt.totalMemory();
            long freeMemory  = rt.freeMemory();
            long usedMemory  = totalMemory - freeMemory;
            double usageRatio = (double) usedMemory / maxMemory;

            if (usageRatio > 0.80) {
                Thread.sleep(10000); // gc 需要时间，等久一点再检测
            }

/*            if (blockStart == 0) {
                blockStart = System.currentTimeMillis();
            }
            long blockedMs = System.currentTimeMillis() - blockStart;

            // 每 5 秒打一次日志，方便观察背压持续时间
            if (blockedMs % 5000 < 500) {
                System.out.printf("[MemGuard] heap high, blocking producer. used=%.1f%% usedMB=%dMB freeMB=%dMB blockedMs=%d%n",
                        usageRatio * 100,
                        usedMemory / 1024 / 1024,
                        (maxMemory - usedMemory) / 1024 / 1024,
                        blockedMs);
            }*/

//            System.gc(); // 达到 90% 才触发，不会频繁

        GlobalSetConfInfo.icebergEngineOperationQueue.put(item);
    }
}
