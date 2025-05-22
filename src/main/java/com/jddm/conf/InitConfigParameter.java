package com.jddm.conf;

import com.dsg.operation.common.ConstantSet;
import com.jddm.boot.StartIcebergEngine;
import com.jddm.common.Constant;
import com.jddm.exception.InitializationException;
import com.publics.common.ConstantPublic;
import com.publics.utils.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * className: InitConfigParameter<br>
 * description: <br>
 * author: wjl<br>
 * date: 2025/5/22 15:16<br>
 */
public class InitConfigParameter {
    public static LinkedHashMap<String,Object> parameterStrMap = new LinkedHashMap<>();
    public Logger log = LogManager.getLogger(InitConfigParameter.class);
    public boolean initConf() throws InitializationException {

        String parameterStr = "";
        boolean initFlag = false;


        int incrementNo = 0;
        File hdfsDir = null;
        try {
            parameterStr = GlobalConfInfo.getConf().getValue("SERVERPORT");
            if (parameterStr != null && !parameterStr.equals("")) {
                Constant.socketServerPort = parameterStr.trim();
                parameterStrMap.put("SERVERPORT",parameterStr.trim());
            }else {
                parameterStrMap.put("SERVERPORT",Constant.socketServerPort);
            }

            parameterStr = GlobalConfInfo.getConf().getValue("THREADPOOLSIZE");
            if (parameterStr != null && !parameterStr.equals("")) {
                Constant.socketThreadPoolSize = parameterStr.trim();
                parameterStrMap.put("THREADPOOLSIZE",parameterStr.trim());
            }else {
                parameterStrMap.put("THREADPOOLSIZE",Constant.socketThreadPoolSize);
            }

            //是否使用表分区
            parameterStr = GlobalConfInfo.getConf().getValue("Hive.Table.Partition.Name");
            if (parameterStr != null && !parameterStr.equals("")) {
                ConstantPublic.setPartitionFlag = true;
                ConstantSet.jddmEngineTablePartitionFlag = true;
                ConstantPublic.setTablePartitionColName = parameterStr.trim();
                parameterStrMap.put("Hive.Table.Partition.Name",parameterStr.trim());
            } else {
                ConstantPublic.setPartitionFlag = false;
                ConstantSet.jddmEngineTablePartitionFlag = false;
            }

            parameterStr = GlobalConfInfo.getConf().getValue("Hive.metastore.uris");
            if (parameterStr != null && !parameterStr.equals("")) {
                Constant.hiveMetastoreUris = parameterStr.trim();
                parameterStrMap.put("Hive.metastore.uris",parameterStr.trim());
            } else {
                log.info("");
                log.info(" Please Setting " + Constant.JddmEngineTypeInfo + " Hive.metastore.uris ......");
                log.info("");
                System.exit(0);
            }

            parameterStr = GlobalConfInfo.getConf().getValue("Hdfs.fs.defaultFS");
            if (parameterStr != null && !parameterStr.equals("")) {
                Constant.fsDefaultInfo = parameterStr.trim();
                parameterStrMap.put("Hdfs.fs.defaultFS",parameterStr.trim());
            }

            parameterStr = GlobalConfInfo.getConf().getValue("HIVE_FILE_COUNT_NO");
            if (parameterStr != null && !parameterStr.equals("")) {
                Constant.writeCountNoToHiveFile = Integer.parseInt(parameterStr.trim());
                parameterStrMap.put("HIVE_FILE_COUNT_NO",Integer.parseInt(parameterStr.trim()));
            } else {
                log.info("");
                log.info(" Setting " + Constant.JddmEngineTypeInfo + " Hive&Hdfs Setting (Dml Data)Row Count To File ! For Example[HIVE_FILE_COUNT_NO=100] ......");
                System.exit(0);
            }

            parameterStr = GlobalConfInfo.getConf().getValue("Hive.Partition.Using.localTime.Flag");
            if (parameterStr != null && !parameterStr.equals("")) {

                if (parameterStr.trim().equalsIgnoreCase("true")) {
                    Constant.hiveTablePartitionUsingLocalTimerFlag = true;
                    parameterStrMap.put("Hive.Partition.Using.localTime.Flag",true);
                }else {
                    parameterStrMap.put("Hive.Partition.Using.localTime.Flag",false);
                }
            }else {
                parameterStrMap.put("Hive.Partition.Using.localTime.Flag",false);
            }

            if (Constant.hiveTablePartitionUsingLocalTimerFlag) {

                parameterStr = GlobalConfInfo.getConf().getValue("Hive.Partition.LocalTime.Format");
                if (parameterStr != null && !parameterStr.equals("")) {
                    Constant.hiveTablePartitionLocalTimerFormatter = parameterStr.trim();
                    parameterStrMap.put("Hive.Partition.LocalTime.Format",parameterStr.trim());
                } else {
                    log.info("");
                    log.info(" Setting " + Constant.JddmEngineTypeInfo + " Hive&Hdfs Setting Local Timer Format ! For Example[Hive.Partition.LocalTime.Format=yyyyMMDDHH] ......");
                    System.exit(0);
                }
            }


            parameterStr = GlobalConfInfo.getConf().getValue("ENGINE_THREAD_TOTAL_SYNC_CONCURRENT");

            if (parameterStr != null && !parameterStr.equals("")) {
                StartIcebergEngine.setTotalSyncNO(Integer.parseInt(parameterStr.trim()));
                parameterStrMap.put("ENGINE_THREAD_TOTAL_SYNC_CONCURRENT",Integer.parseInt(parameterStr.trim()));
            } else {
                log.info("");
                log.info(" Please Setting " + Constant.JddmEngineTypeInfo + " Total Synchronize Number of parallel threads ......");
                log.info("");
                System.exit(0);
            }

            parameterStr = GlobalConfInfo.getConf().getValue("ENGINE_THREAD_INCREMENT_SYNC_CONCURRENT");
            if (parameterStr != null && !parameterStr.equals("")) {
                incrementNo = Integer.parseInt(parameterStr.trim());
                parameterStrMap.put("ENGINE_THREAD_INCREMENT_SYNC_CONCURRENT",Integer.parseInt(parameterStr.trim()));
            } else {

                log.info("");
                log.info(" Please Setting " + Constant.JddmEngineTypeInfo + " Increment Synchronize Number of parallel threads ......");
                log.info("");
                System.exit(0);
            }

            hdfsDir = new File(ConstantSet.baseWorkDir + File.separator + StartIcebergEngine.getHiveFilePath());
            if (hdfsDir.exists()) {

                log.info(" Jddm Iceberg Engine Path ::" + ConstantSet.baseWorkDir + File.separator + StartIcebergEngine.getHiveFilePath() + " exists ... ");
                FileUtils.DeleteFileOrDirectory(hdfsDir);
                //创建HDFS缓存目录
                FileUtils.createDir(ConstantSet.baseWorkDir, StartIcebergEngine.getHiveFilePath());
                FileUtils.createDir(ConstantSet.baseWorkDir, StartIcebergEngine.getHiveFilePath() + File.separator + "bak");
            }
            initFlag = true;
        } catch (Exception e) {
            initFlag = false;
            throw new InitializationException("Config parameter initialization failed", e);

        }
        return initFlag;



    }
    public void printParameters(){
        if (parameterStrMap.isEmpty()) {
            log.info("Application startup parameters: No parameters configured");
            return;
        }

        log.info("=== Startup Parameters ===");
        log.info("Total parameters: {}", parameterStrMap.size());
        log.info("Parameter details:");

        int maxKeyLength = parameterStrMap.keySet().stream()
                .mapToInt(String::length)
                .max()
                .orElse(10);
        // 计算最长的value长度用于对齐

        int maxValueLength = parameterStrMap.values().stream()
                .mapToInt(value -> value != null ? value.toString().length() : 4) // "null"长度为4
                .max()
                .orElse(10);
        String format = "[ %-" + Math.max(maxKeyLength, 15) + "s ]::[ %-"+Math.max(maxValueLength,10)+"s ]";

        parameterStrMap.entrySet()
                .forEach(entry -> {
                    Object value = entry.getValue();
                    String valueStr = value != null ? value.toString() : "null";
                    String formattedLine = String.format(format, entry.getKey(), valueStr);
                    log.info(formattedLine);
                });

        log.info("=== End of Parameters ===");
    }
}