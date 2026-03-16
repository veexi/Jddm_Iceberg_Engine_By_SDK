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
 * 配置参数初始化类：负责从全局 Properties 对象中提取各类运行参数，并将其转化为引擎内部使用的强类型常量。
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
            // 1. 设置通信监听端口
            parameterStr = GlobalConfInfo.getConf().getValue("SERVERPORT");
            if (parameterStr != null && !parameterStr.equals("")) {
                Constant.socketServerPort = parameterStr.trim();
                parameterStrMap.put("SERVERPORT",parameterStr.trim());
            }else {
                parameterStrMap.put("SERVERPORT",Constant.socketServerPort);
            }

            // 2. 设置 Socket 通信的工作线程池大小
            parameterStr = GlobalConfInfo.getConf().getValue("THREADPOOLSIZE");
            if (parameterStr != null && !parameterStr.equals("")) {
                Constant.socketThreadPoolSize = parameterStr.trim();
                parameterStrMap.put("THREADPOOLSIZE",parameterStr.trim());
            }else {
                parameterStrMap.put("THREADPOOLSIZE",Constant.socketThreadPoolSize);
            }

            // 3. 配置 Hive/Iceberg 表分区标志，若设置了分区字段，则引擎将按照指定的物理分区路径组织数据文件
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

            // 5. 数据批次积累阈值：当单线程内存中的待同步行数达到该值时，触发一次 Iceberg Commit 操作。
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
                    log.info("");
                    System.exit(0);
                }
            }

            // 7. Iceberg 写入模式：trajectory (全增量历史) 或 transaction (镜像覆盖同步)
            parameterStr = GlobalConfInfo.getConf().getValue("ICEBERG_WRITE_MODE");
            if (parameterStr != null && !parameterStr.equals("")) {
                String mode = parameterStr.trim().toLowerCase();
                if ("trajectory".equals(mode) || "transaction".equals(mode)) {
                    Constant.icebergWriteMode = mode;
                    parameterStrMap.put("ICEBERG_WRITE_MODE", mode);
                } else {
                    log.warn(" Invalid ICEBERG_WRITE_MODE={}, use default trajectory", parameterStr);
                    parameterStrMap.put("ICEBERG_WRITE_MODE", Constant.icebergWriteMode);
                }
            } else {
                parameterStrMap.put("ICEBERG_WRITE_MODE", Constant.icebergWriteMode);
            }

            // 8. COW 去重机制：batch (基于批次实时合并), timer (基于定时任务异步合并), none (交给查询端去重)
            parameterStr = GlobalConfInfo.getConf().getValue("COW_MODE");
            if (parameterStr != null && !parameterStr.equals("")) {
                String cowMode = parameterStr.trim().toLowerCase();
                if ("batch".equals(cowMode) || "timer".equals(cowMode) || "none".equals(cowMode)) {
                    Constant.cowMode = cowMode;
                    parameterStrMap.put("COW_MODE", cowMode);
                } else {
                    log.warn(" Invalid COW_MODE={}, use default batch", parameterStr);
                    parameterStrMap.put("COW_MODE", Constant.cowMode);
                }
            } else {
                parameterStrMap.put("COW_MODE", Constant.cowMode);
            }

            // 定时任务去重开关
            parameterStr = GlobalConfInfo.getConf().getValue("COMPACT_DEDUP_ENABLED");
            if (parameterStr != null && !parameterStr.equals("")) {
                Constant.compactDedupEnabled = Boolean.parseBoolean(parameterStr.trim());
                parameterStrMap.put("COMPACT_DEDUP_ENABLED", Constant.compactDedupEnabled);
            } else {
                parameterStrMap.put("COMPACT_DEDUP_ENABLED", Constant.compactDedupEnabled);
            }

            // 定时任务小文件合并开关
            parameterStr = GlobalConfInfo.getConf().getValue("COMPACT_SMALL_FILES_ENABLED");
            if (parameterStr != null && !parameterStr.equals("")) {
                Constant.compactSmallFilesEnabled = Boolean.parseBoolean(parameterStr.trim());
                parameterStrMap.put("COMPACT_SMALL_FILES_ENABLED", Constant.compactSmallFilesEnabled);
            } else {
                parameterStrMap.put("COMPACT_SMALL_FILES_ENABLED", Constant.compactSmallFilesEnabled);
            }

            // 定时合并间隔（秒）
            parameterStr = GlobalConfInfo.getConf().getValue("COMPACT_INTERVAL_SECONDS");
            if (parameterStr != null && !parameterStr.equals("")) {
                Constant.compactIntervalSeconds = Integer.parseInt(parameterStr.trim());
                parameterStrMap.put("COMPACT_INTERVAL_SECONDS", Constant.compactIntervalSeconds);
            } else {
                parameterStrMap.put("COMPACT_INTERVAL_SECONDS", Constant.compactIntervalSeconds);
            }

            parameterStr = GlobalConfInfo.getConf().getValue("LOG_LEVEL_TYPE");
            if (parameterStr != null && !parameterStr.equals("")) {
                Constant.debugLogEnabled = "debug".equalsIgnoreCase(parameterStr.trim());
                parameterStrMap.put("LOG_LEVEL_TYPE", parameterStr.trim().toLowerCase());
            } else {
                Constant.debugLogEnabled = false;
                parameterStrMap.put("LOG_LEVEL_TYPE", "info");
            }

            parameterStr = GlobalConfInfo.getConf().getValue("HIVE_FILE_MODIFY_TIMES");
            if (parameterStr != null && !parameterStr.equals("")) {
                Constant.hiveDiffTimers = Integer.parseInt(parameterStr.trim());
                parameterStrMap.put("HIVE_FILE_MODIFY_TIMES", Constant.hiveDiffTimers);
            } else {
                parameterStrMap.put("HIVE_FILE_MODIFY_TIMES", Constant.hiveDiffTimers);
            }

            parameterStr = GlobalConfInfo.getConf().getValue("HIVE_FILE_CACHE_SIZE");
            if (parameterStr != null && !parameterStr.equals("")) {
                Constant.compactSmallFileSizeBytes = Long.parseLong(parameterStr.trim()) * 1024L * 1024L;
                parameterStrMap.put("HIVE_FILE_CACHE_SIZE", parameterStr.trim() + "M");
            } else {
                parameterStrMap.put("HIVE_FILE_CACHE_SIZE", "128M");
            }

            // 11. 引擎并发度配置：负责解析数据包的同步工作线程数量
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

            parameterStr = GlobalConfInfo.getConf().getValue("Engine_Authentication_Mode");
            Constant.kerberosEnabled = "kerberos".equalsIgnoreCase(
                    parameterStr != null ? parameterStr.trim() : "");
            parameterStrMap.put("Engine_Authentication_Mode",
                    Constant.kerberosEnabled ? "Kerberos" : "noKerberos");

            if (Constant.kerberosEnabled) {
                // --- Principal（必填）---
                parameterStr = GlobalConfInfo.getConf().getValue("KERBEROS_USER_PRINCIPAL");
                if (parameterStr == null || parameterStr.equals("")) {
                    log.error(" Engine_Authentication_Mode=Kerberos but KERBEROS_USER_PRINCIPAL is not set !");
                    System.exit(0);
                }
                Constant.kerberosPrincipal = parameterStr.trim();
                parameterStrMap.put("KERBEROS_USER_PRINCIPAL", Constant.kerberosPrincipal);

                parameterStr = GlobalConfInfo.getConf().getValue("KERBEROS_USER_KEYTAB_FILE");
                if (parameterStr == null || parameterStr.equals("")) {
                    log.error(" Engine_Authentication_Mode=Kerberos but KERBEROS_USER_KEYTAB_FILE is not set !");
                    System.exit(0);
                }
                String keytabPath = parameterStr.trim();
                if (!new File(keytabPath).isAbsolute()) {
                    keytabPath = Constant.basicWorkPath + File.separator + "config" + File.separator + keytabPath;
                }
                Constant.kerberosKeytabPath = keytabPath;
                parameterStrMap.put("KERBEROS_USER_KEYTAB_FILE", Constant.kerberosKeytabPath);

                // --- NameNode/Service Principal（选填）---
                parameterStr = GlobalConfInfo.getConf().getValue("ZK_SECURITY_PRINCIPAL_INSTANCE");
                if (parameterStr != null && !parameterStr.equals("")) {
                    Constant.hdfsNamenodePrincipal = parameterStr.trim();
                    parameterStrMap.put("ZK_SECURITY_PRINCIPAL_INSTANCE", Constant.hdfsNamenodePrincipal);
                }

                parameterStr = GlobalConfInfo.getConf().getValue("KERBEROS_KRB5_CONF_FILE");
                if (parameterStr != null && !parameterStr.equals("")) {
                    String krb5Path = parameterStr.trim();
                    if (!new File(krb5Path).isAbsolute()) {
                        krb5Path = Constant.basicWorkPath + File.separator + "config" + File.separator + krb5Path;
                    }
                    Constant.krb5ConfFilePath = krb5Path;
                    parameterStrMap.put("KERBEROS_KRB5_CONF_FILE", Constant.krb5ConfFilePath);
                }

                parameterStr = GlobalConfInfo.getConf().getValue("HIVE_METASTORE_KERBEROS_PRINCIPAL");
                if (parameterStr == null || parameterStr.equals("")) {
                    log.error(" Engine_Authentication_Mode=Kerberos but HIVE_METASTORE_KERBEROS_PRINCIPAL is not set !");
                    System.exit(0);
                }
                Constant.hiveMetastorePrincipal = parameterStr.trim();
                parameterStrMap.put("HIVE_METASTORE_KERBEROS_PRINCIPAL", Constant.hiveMetastorePrincipal);
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

            parameterStr = GlobalConfInfo.getConf().getValue("DROP_TABLE_FLAG");
            if (parameterStr != null && !parameterStr.equals("")) {
                Constant.dropTableFlag = Boolean.parseBoolean(parameterStr.trim());
                parameterStrMap.put("DROP_TABLE_FLAG", Constant.dropTableFlag);
            } else {
                parameterStrMap.put("DROP_TABLE_FLAG", Constant.dropTableFlag);
            }

            hdfsDir = new File(ConstantSet.baseWorkDir + File.separator + StartIcebergEngine.getHiveFilePath());
            if (hdfsDir.exists()) {
                log.info(" Jddm Iceberg Engine Path ::" + ConstantSet.baseWorkDir + File.separator + StartIcebergEngine.getHiveFilePath() + " exists ... ");
                FileUtils.DeleteFileOrDirectory(hdfsDir);
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

        int maxValueLength = parameterStrMap.values().stream()
                .mapToInt(value -> value != null ? value.toString().length() : 4)
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