package com.jddm.boot;

import com.dsg.analysis.tableInfo.vo.SourceTableInfoVo;
import com.dsg.analysis.tableInfo.vo.TableInfoVo;
import com.dsg.analysis.vo.PackageReturnVo;
import com.dsg.operation.common.ConstantSet;
import com.jddm.common.Constant;
import com.jddm.conf.Configuration;
import com.jddm.conf.GlobalConfInfo;
import com.jddm.conf.GlobalSetConfInfo;
import com.jddm.conf.InitConfigParameter;
import com.jddm.exception.InitializationException;
import com.jddm.killOper.JddmEngineKillHandler;
import com.jddm.manager.ServiceManager;
import com.jddm.operation.ddl.IceBergTableOperationByEngine;
import com.jddm.operation.timer.ReLoaderTableInfoBySqliteDB;
import com.jddm.operation.timer.TimerByHiveCacheFileThread;
import com.jddm.thread.OperationTotalSyncByIceBergThreadPool;
import com.jddm.utils.IcebergValidator;
import com.publics.cache.TableAllCacheInfo;
import com.publics.common.ConstantPubSet;
import com.publics.common.ConstantPublic;
import com.publics.conf.GlobalConfCommInfo;
import com.publics.engine.operation.socketSecGeneration.SocketGeneralEngine;
import com.publics.engine.state.EngineStateInfo;
import com.publics.operation.jdbcOper.embeddedDB.SQLiteJDBC;
import com.publics.operation.socket.ExecSQLInfoVo;
import com.publics.utils.FileUtils;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.tools.picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
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

/**
 * className: StartIcebergEngine<br>
 * description: <br>
 * author: wjl<br>
 * date: 2025/5/20 15:32<br>
 */
public class StartIcebergEngine {
    private final static String CONFIG_INFO_PATH ="config/config.properties";
    private final static String LOG_XML_PATH = "config/log4jConfig.xml";
    private static  String hiveFilePath = "hdfsFile";
    private static int totalSyncNO=1;
    public static final String version = IcebergValidator.class.getPackage().getImplementationVersion();
    /*** *** *** ***
     * @functionName（方法名称）: main
     * @description （方法说明）: 启动入口
     * @param       （传入参数）: String[] args
     * @return      （返回）   : null
     * @exception   （异常）   : Exception
     * @author      （创建人）: mjddw
     * @since       （创建时间）: 2025/5/22 16:55
     ***/
    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();
        try {

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


            properties.setProperty("ServerPort", Constant.socketServerPort);
            properties.setProperty("ThreadPoolSize", Constant.socketThreadPoolSize);
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
                                    sqLiteJDBC.recordJddmCacheTable_ToSqliteDB(cachekeyName,((TableInfoVo) item).getObjn()+"",content,tableCacheInfo.serializableTableVo_ToByteArray((TableInfoVo) item));

                                }
                                GlobalConfCommInfo.cacheSourceTableInfoMap.remove(cachekeyName);
                                if (!GlobalConfCommInfo.cacheTableInfoMap.containsKey(cachekeyName)){
                                    IceBergTableOperationByEngine iceBergTableOperationByEngine = new IceBergTableOperationByEngine();
                                    iceBergTableOperationByEngine.importHiveTable_IceBerg_Table((TableInfoVo) item, tableColumnMap);
                                }
                                GlobalConfCommInfo.cacheTableInfoMap.put(cachekeyName, (TableInfoVo) item);

                                break;
                            case "PackageReturnVo":
                                System.out.println(" DML ::" + ((PackageReturnVo) item).getOwnerName() + "." + ((PackageReturnVo) item).getTableName() + " fileNo ::" + ((PackageReturnVo) item).getFileNo() + " Rows ::" + ((PackageReturnVo) item).getRowsCount());
                                try {
                                    GlobalSetConfInfo.icebergEngineOperationQueue.put((PackageReturnVo) item);
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
/*** *** *** ***
 * @functionName（方法名称）: initializeServices
 * @description （方法说明）: 初始化各个服务
 * @param       （传入参数）: null
 * @return      （返回）   : boolean
 * @exception   （异常）   : InitializationException
 * @author      （创建人）: mjddw
 * @since       （创建时间）: 2025/5/22 16:53
 ***/
    private static boolean initializeServices() throws InitializationException {
        boolean returnFlag = false;
        ExecutorService fixedThreadPool = null;
        ScheduledExecutorService scheduledThreadPool = null;
        ReLoaderTableInfoBySqliteDB reLoaderTableInfoBySqliteDB = null;

        try {
            JddmEngineKillHandler jddmKillHandler = new JddmEngineKillHandler(5, TimeUnit.SECONDS);
            jddmKillHandler.registerSignal("TERM");
            jddmKillHandler.registerSignal("INT");

            scheduledThreadPool = Executors.newScheduledThreadPool(5);
            TimerByHiveCacheFileThread timerByHiveCacheFileThread = new TimerByHiveCacheFileThread();
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
        IcebergValidator icebergValidator = new IcebergValidator();
        try {
            icebergValidator.validateTableOperations();
        }catch (Exception e){
            throw new RuntimeException("Iceberg connection validate failed",e);
        }finally {
            icebergValidator=null;
        }

        log.info("Jddm Iceberg Engine started successfully.");
        log.info("...");
    }

    public static String getBuildTimeString(){
        String returnTime = "";
        try (InputStream in =
                     IcebergValidator.class.getClassLoader()
                             .getResourceAsStream("META-INF/MANIFEST.MF")) {
            Manifest mf = new Manifest(in);
            String timestamp = mf.getMainAttributes()
                    .getValue("Build-Timestamp");
            OffsetDateTime utc = OffsetDateTime.parse(timestamp);
            // 2. 改变偏移量到 +08:00（同一时刻）
            OffsetDateTime beijing = utc.withOffsetSameInstant(ZoneOffset.ofHours(8));
            returnTime = beijing.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        } catch (IOException e) {
            throw new RuntimeException(e);
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
}
