package com.jddm.boot;

import com.dsg.analysis.tableInfo.vo.SourceTableInfoVo;
import com.dsg.analysis.tableInfo.vo.TableInfoVo;
import com.dsg.analysis.vo.PackageReturnVo;
import com.dsg.operation.common.ConstantSet;
import com.jddm.common.Constant;
import com.jddm.conf.Configuration;
import com.jddm.conf.GlobalConfInfo;
import com.jddm.conf.GlobalSetConfInfo;
import com.jddm.killOper.JddmEngineKillHandler;
import com.jddm.operation.ddl.IceBergTableOperationByEngine;
import com.jddm.operation.timer.ReLoaderTableInfoBySqliteDB;
import com.jddm.operation.timer.TimerByHiveCacheFileThread;
import com.jddm.thread.OperationTotalSyncByIceBergThreadPool;
import com.publics.common.ConstantPublic;
import com.publics.engine.operation.socketSecGeneration.SocketGeneralEngine;
import com.publics.operation.socket.ExecSQLInfoVo;
import com.publics.utils.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * className: StartIcebergEngine<br>
 * description: <br>
 * author: wjl<br>
 * date: 2025/5/20 15:32<br>
 */
public class StartIcebergEngine {
    private final static String CONFIG_INFO_PATH ="config/config.properties";
    private final static String LOG_XML_PATH = "config/log4jConfig.xml";
    private static final String hiveFilePath = "hdfsFile";
    private static int totalSyncNO=1;
    public static void main(String[] args) {
        Properties properties = new Properties();
        ExecutorService fixedThreadPool = null;
        ScheduledExecutorService scheduledThreadPool = null;
        ReLoaderTableInfoBySqliteDB reLoaderTableInfoBySqliteDB = null;

        Properties props=System.getProperties(); //系统属性
        Constant.basicWorkPath=props.getProperty("user.dir");
        ConstantSet.baseWorkDir=Constant.basicWorkPath;

        Configuration conf = new Configuration(CONFIG_INFO_PATH);
        GlobalConfInfo.setConf(conf);

        initConf();
        JddmEngineKillHandler jddmKillHandler= new JddmEngineKillHandler(5,TimeUnit.SECONDS);
        jddmKillHandler.registerSignal("TERM");
        jddmKillHandler.registerSignal("INT");

        scheduledThreadPool = Executors.newScheduledThreadPool(1);
        TimerByHiveCacheFileThread timerByHiveCacheFileThread = new TimerByHiveCacheFileThread();
        //表示延迟5秒后每3秒执行一次。
        scheduledThreadPool.scheduleAtFixedRate(timerByHiveCacheFileThread, 5, 30, TimeUnit.SECONDS);

        scheduledThreadPool = Executors.newScheduledThreadPool(3);
        //设置reloaded DDL的计算器
        GlobalConfInfo.reloadTableVoCalcMap.put(Constant.reloadKeyName, new AtomicInteger(0));
        reLoaderTableInfoBySqliteDB= new ReLoaderTableInfoBySqliteDB();
        scheduledThreadPool.scheduleAtFixedRate(reLoaderTableInfoBySqliteDB, 5, 5, TimeUnit.SECONDS);

        fixedThreadPool = Executors.newFixedThreadPool(50);

        OperationTotalSyncByIceBergThreadPool operationTotalSyncByIceBergThreadPool = null;
        for(int i=0;i<totalSyncNO;i++){

            operationTotalSyncByIceBergThreadPool = new OperationTotalSyncByIceBergThreadPool();
            fixedThreadPool.execute(operationTotalSyncByIceBergThreadPool);
        }

        properties.setProperty("ServerPort","19990");
        properties.setProperty("ThreadPoolSize","200");
        SocketGeneralEngine engine = SocketGeneralEngine.create("path")
                .setting(properties)
                .notifying((item) -> {
                    //DataVo dataVo = (DataVo) item;

                    switch(item.getClass().getSimpleName()) {
                        case "ExecSQLInfoVo":  //if(item instanceof ExecSQLInfoVo)
                            System.out.println(" EXEC_SQL :: "+item.getClass().getSimpleName()+" --->"+new String(((ExecSQLInfoVo)item).getExecSqlArr()));
                            break;
                        case "SourceTableInfoVo": // } else if(item instanceof SourceTableInfoVo) {
                            System.out.println(" Source_DDL ::"+((SourceTableInfoVo)item).getOwner()+"."+((SourceTableInfoVo)item).getTableName()+" colSize ::"+((SourceTableInfoVo)item).getColumnList().size());
                            try{
                                System.out.println(" Jddm Engine Plug-in  Source Table DDL schema&table    ::" + ((SourceTableInfoVo)item).getOwner() + "." + ((SourceTableInfoVo)item).getTableName());
                                System.out.println(" Jddm Engine Plug-in  Source Table DDL ColumnSize   ::" + ((SourceTableInfoVo)item).getColumnList().size());
                            }catch (Exception e){

                            }
                            break;
                        case "TableInfoVo": //} else if(item instanceof TableInfoVo) {
                            System.out.println(" DDL ::"+((TableInfoVo)item).getOwner()+"."+((TableInfoVo)item).getTableName()+" colSize ::"+((TableInfoVo)item).getColumnList().size());
                            Map<String, String> tableColumnMap = new LinkedHashMap<String,String>();
                            IceBergTableOperationByEngine iceBergTableOperationByEngine =  new IceBergTableOperationByEngine();
                            iceBergTableOperationByEngine.importHiveTable_IceBerg_Table((TableInfoVo)item, tableColumnMap);
                            break;
                        case "PackageReturnVo":
                            System.out.println(" DML ::"+((PackageReturnVo)item).getOwnerName()+"."+((PackageReturnVo)item).getTableName()+" fileNo ::"+((PackageReturnVo)item).getFileNo()+" Rows ::"+((PackageReturnVo)item).getRowsCount());
                            try {
                                GlobalSetConfInfo.icebergEngineOperationQueue.put((PackageReturnVo)item);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            break;
                        default:
                            System.out.println(" OtherName ::"+item.getClass().getSimpleName()+" ---> "+item.toString());
                            break;
                    }

                })
                .build();
        engine.run();


    }
    public static void initConf(){
        Logger log = LogManager.getLogger(StartIcebergEngine.class);
        String parameterStr = "";

        int incrementNo = 0;
        File hdfsDir = null;

        //是否使用表分区
        parameterStr = GlobalConfInfo.getConf().getValue("Hive.Table.Partition.Name");
        if(parameterStr != null && !parameterStr.equals("")){
            ConstantPublic.setPartitionFlag = true;
            ConstantSet.jddmEngineTablePartitionFlag = true;
            ConstantPublic.setTablePartitionColName = parameterStr.trim();
        } else {
            ConstantPublic.setPartitionFlag = false;
            ConstantSet.jddmEngineTablePartitionFlag = false;
        }

        parameterStr = GlobalConfInfo.getConf().getValue("Hive.metastore.uris");
        if(parameterStr != null && !parameterStr.equals("")){
            Constant.hiveMetastoreUris = parameterStr.trim();
        }else {
            log.info("");
            log.info(" Please Setting "+Constant.JddmEngineTypeInfo+" Hive.metastore.uris ......");
            log.info("");
            System.exit(0);
        }

        parameterStr = GlobalConfInfo.getConf().getValue("Hdfs.fs.defaultFS");
        if(parameterStr !=null && !parameterStr.equals("")){

            Constant.fsDefaultInfo = parameterStr.trim();
        }

        parameterStr = GlobalConfInfo.getConf().getValue("HIVE_FILE_COUNT_NO");
        if(parameterStr != null && !parameterStr.equals("")){

            Constant.writeCountNoToHiveFile = Integer.parseInt(parameterStr.trim());
        }else {
            log.info("");
            log.info(" Setting "+Constant.JddmEngineTypeInfo+" Hive&Hdfs Setting (Dml Data)Row Count To File ! For Example[HIVE_FILE_COUNT_NO=100] ......");
            System.exit(0);
        }

        parameterStr = GlobalConfInfo.getConf().getValue("Hive.Partition.Using.localTime.Flag");
        if(parameterStr != null && !parameterStr.equals("")){

            if(parameterStr.trim().equalsIgnoreCase("true")) {
                Constant.hiveTablePartitionUsingLocalTimerFlag=true;
            }
        }

        if(Constant.hiveTablePartitionUsingLocalTimerFlag) {

            parameterStr = GlobalConfInfo.getConf().getValue("Hive.Partition.LocalTime.Format");
            if(parameterStr != null && !parameterStr.equals("")){
                Constant.hiveTablePartitionLocalTimerFormatter=parameterStr.trim();
            }else {
                log.info("");
                log.info(" Setting "+Constant.JddmEngineTypeInfo+" Hive&Hdfs Setting Local Timer Format ! For Example[Hive.Partition.LocalTime.Format=yyyyMMDDHH] ......");
                System.exit(0);
            }
        }


        parameterStr = GlobalConfInfo.getConf().getValue("ENGINE_THREAD_TOTAL_SYNC_CONCURRENT");

        if(parameterStr != null && !parameterStr.equals("")){
            totalSyncNO = Integer.parseInt(parameterStr.trim());
        }else {
            log.info("");
            log.info(" Please Setting "+Constant.JddmEngineTypeInfo+" Total Synchronize Number of parallel threads ......");
            log.info("");
            System.exit(0);
        }

        parameterStr = GlobalConfInfo.getConf().getValue("ENGINE_THREAD_INCREMENT_SYNC_CONCURRENT");
        if(parameterStr !=null && !parameterStr.equals("")){
            incrementNo = Integer.parseInt(parameterStr.trim());
        }else {

            log.info("");
            log.info(" Please Setting "+Constant.JddmEngineTypeInfo+" Increment Synchronize Number of parallel threads ......");
            log.info("");
            System.exit(0);
        }

        hdfsDir = new File(ConstantSet.baseWorkDir+File.separator+hiveFilePath);
        if(hdfsDir.exists()) {

            log.info(" JddmEngine hive&hdfs Path ::"+ConstantSet.baseWorkDir+File.separator+hiveFilePath+" exists ... ");
            FileUtils.DeleteFileOrDirectory(hdfsDir);
            //创建HDFS缓存目录
            FileUtils.createDir(ConstantSet.baseWorkDir,hiveFilePath);
            FileUtils.createDir(ConstantSet.baseWorkDir,hiveFilePath+File.separator+"bak");
        }

    }
}
