package com.jddm.operation.ddl;

import com.dsg.analysis.tableInfo.vo.TableColumnVo;
import com.dsg.analysis.tableInfo.vo.TableInfoVo;
import com.dsg.analysis.utils.ConversionUtil;
import com.dsg.operation.common.ConstantSet;
import com.jddm.common.Constant;
import com.jddm.common.ConstantColType;
import com.jddm.conf.GlobalConfInfo;
import com.jddm.conf.GlobalSetConfInfo;
import com.jddm.thread.OperationTotalSyncByIceBergThreadPool;
import com.jddm.utils.KerberosAuthUtil;
import com.publics.common.ConstantPubSet;
import com.publics.common.ConstantPublic;
import com.publics.conf.GlobalConfCommInfo;
import com.publics.utils.FileUtils;
import com.publics.vo.SocketReturnVo;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.jddm.thread.OperationTotalSyncByIceBergThreadPool.*;

public class IceBergTableOperationByEngine {

    //	private static Logger log = LogManager.getLogger(IceBergTableOperationByEngine.class);
    public Logger log = LogManager.getLogger(IceBergTableOperationByEngine.class);


    /**
     * 在 Hive Catalog 中创建或同步 Iceberg 表。
     * 该方法负责：
     * 1. 解析源表元数据（字段名、类型、主键）。
     * 2. 将源库类型映射为 Iceberg Schema。
     * 3. 根据主键和写入模式（v2 表）配置 Equality Delete 策略。
     * 4. 执行原子的表创建或加载操作，并缓存表句柄。
     *
     * @param tableInfoVo 源表结构化信息
     * @param thisTableColMap 当前已识别映射的字段缓存
     * @return 包含操作结果的通用返回对象
     */
    public SocketReturnVo importHiveTable_IceBerg_Table(TableInfoVo tableInfoVo,Map<String,String> thisTableColMap){

        SocketReturnVo socketReturnVo = new SocketReturnVo();
        String hiveExtTableName="";

        String setTableKeyName="";
        Schema iceBergSchema = null;
        setTableKeyName = tableInfoVo.getOwner().toLowerCase()+"."+tableInfoVo.getTableName().toLowerCase();
        StringBuffer columnSqlString = new StringBuffer();
        StringBuffer addColumnInfoString = new StringBuffer();
        StringBuffer columnNameString = new StringBuffer();
        StringBuffer createSqlString = new StringBuffer();
        StringBuffer insertSqlString = new StringBuffer();
        StringBuffer insertSelectString = new StringBuffer();
        List<TableColumnVo> columnList = tableInfoVo.getColumnList();

        List<TableColumnVo> operColumnList = new ArrayList<TableColumnVo>();
        List<TableColumnVo> yloaderColumnList = new ArrayList<TableColumnVo>();

        if(ConstantPubSet.logForAgentType == 2000){
            log.info("[DDL] ICEBerg Schema ::"+tableInfoVo.getOwner().toLowerCase()+" TName ::"+tableInfoVo.getTableName().toLowerCase()+" tableSpace ::"+tableInfoVo.getTableSpace());
        }


        List<Integer> pkColumnList = tableInfoVo.getPkColumnNo();
        Map<String,String> pkColumnMap=new HashMap<String,String>();
        // 第一步：解析并提取主键（PK）定义
        // 主键信息对于 Transaction 模式下的数据去重和 Delete File 生成至关重要。
        for( TableColumnVo columnVo: columnList){

            if(thisTableColMap.containsKey(columnVo.getColumnName().toLowerCase())){
                thisTableColMap.remove(columnVo.getColumnName().toLowerCase());
            }

            if(pkColumnList !=null && !pkColumnList.isEmpty()){

                for(Integer pkNo :pkColumnList){

                    if(pkNo == columnVo.getColumnNo()){
                        log.info("[DDL] Table: {} ,PrimaryKeyName: {} ,pkNo: {}",setTableKeyName,columnVo.getColumnName().toLowerCase(),pkNo);
                        pkColumnMap.put(columnVo.getColumnName().toLowerCase(), pkNo+"");
                        break;
                    }
                }
            }
        }
        List<String> pkNames = new ArrayList<>(pkColumnMap.keySet());
        GlobalSetConfInfo.TablePkColCacheMap.put(setTableKeyName, pkNames);
        // 第二步：识别物理分区字段
        // 如果开启了分区标志，我们将验证配置的分区字段是否存在于当前表结构中。
        String partColName = ConstantPublic.setPartitionFlag ?
                ConstantPublic.setTablePartitionColName.toLowerCase().trim() : null;
        if (partColName != null && !partColName.isEmpty()) {
            boolean partColExists = false;
            for (TableColumnVo col : columnList) {
                if (col.getColumnName().equalsIgnoreCase(partColName)) {
                    partColExists = true;
                    break;
                }
            }
            if (!partColExists) {
                log.warn("[DDL] table={} partition col '{}' not found, fallback unpartitioned",
                        setTableKeyName, partColName);
                partColName = null;
            } else {
                log.info("[DDL] table={} partition col='{}' found, will use identity partition",
                        setTableKeyName, partColName);
            }
        }
        if(!thisTableColMap.isEmpty()){

            TableColumnVo tableColumnVo=null;
            for(Map.Entry<String, String> addColMap:thisTableColMap.entrySet()){

                log.info("[DDL] List_Size ::"+columnList.size()+" ################ ADD COLUMN ::"+addColMap.getKey()+" Type ::"+addColMap.getValue());

                if(ConstantPublic.setPartitionFlag){
                    if(addColMap.getKey().equalsIgnoreCase(ConstantPublic.setTablePartitionColName)){
                        break;
                    }
                }else{
                    if(addColMap.getKey().equalsIgnoreCase("busi_date")){
                        break;
                    }
                }

                switch(addColMap.getValue().toUpperCase()){
                    case "DATE":
                        tableColumnVo = new TableColumnVo();
                        tableColumnVo.setColumnName(addColMap.getKey().toLowerCase());
                        tableColumnVo.setColumnType(12);

                        operColumnList.add(tableColumnVo);
                        //columnList.add(tableColumnVo);
                        break;
                    case "VARCHAR2":
                        tableColumnVo = new TableColumnVo();
                        tableColumnVo.setColumnName(addColMap.getKey().toLowerCase());
                        tableColumnVo.setColumnType(1);
                        operColumnList.add(tableColumnVo);
                        //columnList.add(tableColumnVo);
                        break;
                    case "NUMBER":
                        tableColumnVo = new TableColumnVo();
                        tableColumnVo.setColumnName(addColMap.getKey().toLowerCase());
                        tableColumnVo.setColumnType(2);
                        tableColumnVo.setNumberType(1000);
                        operColumnList.add(tableColumnVo);
                        //columnList.add(tableColumnVo);
                        break;
                }
            }
        }



        for( TableColumnVo columnVo: columnList){
            if (ConstantPubSet.logForAgentType == 2000) {
                log.info("[DDL] --dict[ALL FieldType]-- >>> "+columnVo.getColumnName().toLowerCase()+" value ::"+columnVo.getColumnType()+" NumberType ::"+columnVo.getNumberType()+" columnNo ::"+columnVo.getColumnNo()+" commons ::"+columnVo.getColComment()+" ("+columnVo.getColumnLen()+","+columnVo.getColPrecision()+")");

            }

            if(columnVo.isAddColFlag()) {
                if(ConstantPublic.setPartitionFlag){
                    if(columnVo.getColumnName().toLowerCase().equalsIgnoreCase(ConstantPublic.setTablePartitionColName)){
                        log.info("[DDL] JddmEngine ######## Partition COLUMN ::"+columnVo.getColumnName().toLowerCase()+" Type ::"+columnVo.getColumnType());
                        yloaderColumnList.add(columnVo);
                        continue;
                    }else {
                        log.info("[DDL] JddmEngine ######## Partition ADD COLUMN ::"+columnVo.getColumnName().toLowerCase()+" Type ::"+columnVo.getColumnType());
                    }
                }else {
                    log.info("[DDL] JddmEngine ################ ADD COLUMN ::"+columnVo.getColumnName().toLowerCase()+" Type ::"+columnVo.getColumnType());
                }

                if(columnVo.getColComment() !=null) {
                    addColumnInfoString.append("`"+columnVo.getColumnName().toLowerCase()+"` "+ ConstantColType.OraTypeMappingSet.ora_0x01.getHiveColType()+"  comment '"+columnVo.getColComment()+"',");
                }else {
                    addColumnInfoString.append("`"+columnVo.getColumnName().toLowerCase()+"` "+ConstantColType.OraTypeMappingSet.ora_0x01.getHiveColType()+",");
                }

                yloaderColumnList.add(columnVo);
            }else {
                operColumnList.add(columnVo);
            }
        }

        log.info("[DDL] JDDM_ICEBERG_Engine List_Size ::"+columnList.size()+" operationList ::"+operColumnList.size());


        hiveExtTableName = tableInfoVo.getOwner().toLowerCase()+"."+FileUtils.createTableName_ByJddmEngine(tableInfoVo.getOwner().toLowerCase(),tableInfoVo.getTableName().toLowerCase());



        log.info("[DDL] JDDM_ICEBERG_Engine Table::["+hiveExtTableName+"] List_Size ::"+columnList.size()+" operationList ::"+operColumnList.size());

        if(ConstantPublic.setPartitionFlag){
            insertSelectString.append("INSERT INTO "+hiveExtTableName+ " partition("+ConstantPublic.setTablePartitionColName+"=@dsg_date@) ( ");
        }else{
            insertSelectString.append("INSERT INTO "+hiveExtTableName+ " partition(busi_date=@dsg_date@) ( ");
        }

        insertSqlString.append("INSERT INTO "+hiveExtTableName+ " VALUES (");
        createSqlString.append("create table IF NOT EXISTS "+hiveExtTableName+" ( ");

        GlobalConfInfo.jddmEngineByHiveTableCacheMap.put(tableInfoVo.getOwner().toLowerCase()+"."+tableInfoVo.getTableName().toLowerCase(), hiveExtTableName);


        List<Types.NestedField> iceBergTablefields = new ArrayList<>();

        // =====================================================================
        // 主循环：遍历 operColumnList，将每个 Oracle 字段映射为 Iceberg NestedField。
        // 类型映射规则以客户提供的 Oracle→Iceberg 映射表为准；
        // 不在映射表内的类型统一降级为 StringType。
        // =====================================================================
        for( TableColumnVo columnVo: operColumnList){

            if(ConstantPublic.setPartitionFlag){
                if(!columnVo.getColumnName().toLowerCase().equalsIgnoreCase(ConstantPublic.setTablePartitionColName)){
                    columnNameString.append(columnVo.getColumnName().toLowerCase()+",");
                }
            }else{
                if(!columnVo.getColumnName().toLowerCase().equalsIgnoreCase("busi_date")){
                    columnNameString.append(columnVo.getColumnName().toLowerCase()+",");
                }else{
                    continue;
                }
            }

            Types.NestedField nestedField = null;

            if (ConstantPubSet.logForAgentType == 2000) {
                log.info("[DDL] --reload[ALL FieldType]-- >>> "+columnVo.getColumnName().toLowerCase()+" value ::"+columnVo.getColumnType()+" NumberType ::"+columnVo.getNumberType()+" columnNo ::"+columnVo.getColumnNo()+" commons ::"+columnVo.getColComment()+" ("+columnVo.getColumnLen()+","+columnVo.getColPrecision()+")");
            }

            // 关键逻辑：类型映射与主键强制约束。
            // transaction 模式下主键字段标记为 required，确保 Equality Delete 完整性。
            boolean isTransactionMode = "transaction".equals(Constant.icebergWriteMode);
            boolean isPkCol = isTransactionMode && pkColumnMap.containsKey(columnVo.getColumnName().toLowerCase());

            // CLOB(112)/BLOB(113)：保留 sourceType==5009(bytes) 的写入层标记
            if (columnVo.getColumnType() == 112 || columnVo.getColumnType() == 113) {
                int sourceTypeInt = ConversionUtil.getIntFromBytes(columnVo.getSourceType());
                if (sourceTypeInt == 5009) {
                    GlobalConfCommInfo.jddmEngineTypeBy0x71BytesColMap.put(
                            setTableKeyName + "." + columnVo.getColumnName().toLowerCase(),
                            columnVo.getColumnName().toLowerCase());
                }
            }

            // 通过统一的类型解析方法得到 Iceberg 物理类型
            org.apache.iceberg.types.Type fieldType = resolveIcebergType(columnVo, setTableKeyName);

            nestedField = isPkCol
                    ? Types.NestedField.required(iceBergTablefields.size() + 1,
                    columnVo.getColumnName().toLowerCase(), fieldType)
                    : Types.NestedField.optional(iceBergTablefields.size() + 1,
                    columnVo.getColumnName().toLowerCase(), fieldType);
            iceBergTablefields.add(nestedField);

            // legacy Hive JDBC INSERT SQL 占位符（仅 number 类型在原始逻辑中追加）
            if (columnVo.getColumnType() == 2) {
                insertSqlString.append("?,");
            }
        }

        // =====================================================================
        // yloader 附加列循环：同样走 resolveIcebergType，与 operColumnList 保持一致。
        // =====================================================================
        for(TableColumnVo columnVo: yloaderColumnList) {
            boolean isTransactionMode = "transaction".equals(Constant.icebergWriteMode);
            boolean isPkCol = isTransactionMode && pkColumnMap.containsKey(columnVo.getColumnName().toLowerCase());
            GlobalConfCommInfo.jddmEngineTypeByYloaderColMap.put(
                    setTableKeyName + "." + columnVo.getColumnName().toLowerCase(),
                    columnVo.getColumnName().toLowerCase());

            org.apache.iceberg.types.Type fieldType = resolveIcebergType(columnVo, setTableKeyName);

            Types.NestedField nestedField = isPkCol
                    ? Types.NestedField.required(iceBergTablefields.size() + 1,
                    columnVo.getColumnName().toLowerCase(), fieldType)
                    : Types.NestedField.optional(iceBergTablefields.size() + 1,
                    columnVo.getColumnName().toLowerCase(), fieldType);
            iceBergTablefields.add(nestedField);
        }

        iceBergSchema = new Schema(iceBergTablefields);
        // 建完 Schema 后，把每列的类型预存到 columnTypeCache，供 fillRecord O(1) 查询
        for (org.apache.iceberg.types.Types.NestedField f : iceBergSchema.columns()) {
            GlobalSetConfInfo.columnTypeCache.put(setTableKeyName + "." + f.name(), f.type());
        }
        log.info("[DDL] columnTypeCache filled for table={} cols={}", setTableKeyName, iceBergSchema.columns().size());
        log.info("[DDL] Successfully created Schema: "+iceBergSchema.toString());

        GlobalSetConfInfo.IceBergSchemaCahceMap.put(setTableKeyName, iceBergSchema);

        if(ConstantSet.jddmEngineTablePartitionFlag){
            if(ConstantPublic.setPartitionFlag){
                createSqlString.append(") partitioned by ("+ConstantPublic.setTablePartitionColName+" bigint)");
            } else {
                createSqlString.append(") partitioned by (busi_date bigint)");
            }

        } else {
            createSqlString.append(")");
        }

        try {

            Table iceBergTable;
            PartitionSpec spec = null;
            TableIdentifier tableIdentifier = TableIdentifier.of(tableInfoVo.getOwner().toLowerCase(),tableInfoVo.getTableName().toLowerCase());

            log.info(" ========== JDBC IceBerg DDL_CreateSQL :::"+tableIdentifier.toString());
            HiveCatalog catalog = new HiveCatalog();
            Configuration hadoopConf = KerberosAuthUtil.buildHadoopConf();
            String dynamicHiveSitePath = Constant.basicWorkPath + java.io.File.separator + "config" + java.io.File.separator + "hive-site.xml";
            hadoopConf.addResource(new org.apache.hadoop.fs.Path(dynamicHiveSitePath));
            hadoopConf.set("fs.defaultFS", Constant.fsDefaultInfo);
            catalog.setConf(hadoopConf);
            Map<String, String> properties = new HashMap<String, String>();
            properties.put(CatalogProperties.WAREHOUSE_LOCATION, Constant.fsDefaultInfo);
            properties.put(CatalogProperties.URI, Constant.hiveMetastoreUris);
            properties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.hive.HiveCatalog");
            properties.put("format-version", "2");

            catalog.initialize("hive", properties);
            log.info("[DDL] Original config fsDefaultInfo : {} ,hiveMetastoreUris : {}", Constant.fsDefaultInfo, Constant.hiveMetastoreUris);
            log.info("[DDL] HiveCatalog Initialize Properties : {}", properties);

            // 第四步：构建分区规格（Partition Spec）。
            // 采用 Identity 分区，即 Data File 的目录结构直接由分区字段的值决定。
            if (partColName != null) {
                try {
                    spec = PartitionSpec.builderFor(GlobalSetConfInfo.IceBergSchemaCahceMap.get(setTableKeyName))
                            .identity(partColName)
                            .build();
                    log.info("[DDL] table={} partition by identity col='{}'", setTableKeyName, partColName);
                } catch (Exception e) {
                    log.warn("[DDL] table={} partition spec build failed col='{}', fallback unpartitioned. err={}",
                            setTableKeyName, partColName, e.getMessage());
                    spec = PartitionSpec.unpartitioned();
                }
            } else {
                spec = PartitionSpec.unpartitioned();
                log.info("[DDL] table={} unpartitioned", setTableKeyName);
            }
            Namespace ns = Namespace.of(tableInfoVo.getOwner().toLowerCase());

            if (!catalog.namespaceExists(ns)) {
                catalog.createNamespace(ns);
                log.info("[DDL] Created missing namespace: {}", ns);
            }
            log.info("[DDL] Begin creating Iceberg table name: {}", tableIdentifier);
            boolean isTableExists = false;
            try {
                isTableExists = catalog.tableExists(tableIdentifier);
            } catch (Exception e) {
                String errMsg = e.getMessage();
                Throwable cause = e.getCause();
                while (cause != null) {
                    if (cause.getMessage() != null) {
                        errMsg += " " + cause.getMessage();
                    }
                    cause = cause.getCause();
                }

                if (errMsg != null && errMsg.contains("Operation category READ is not supported in state standby")) {
                    log.warn("[DDL] Caught StandbyException during tableExists check. Assuming table exists to trigger drop/recreate logic. Error: {}", e.getMessage());
                    isTableExists = true;
                } else {
                    throw e;
                }
            }
            if (!isTableExists) {
                properties.put("engine.hive.enabled", "true");
                if (!pkNames.isEmpty() && "transaction".equals(Constant.icebergWriteMode)) {
                    properties.put(TableProperties.DELETE_MODE, "merge-on-read");
                    properties.put(TableProperties.UPDATE_MODE, "merge-on-read");
                    properties.put(TableProperties.MERGE_MODE,  "merge-on-read");
                    log.info("[DDL] table={} pk={} set merge-on-read", setTableKeyName, pkNames);
                }
                iceBergTable = catalog.createTable(tableIdentifier, GlobalSetConfInfo.IceBergSchemaCahceMap.get(setTableKeyName), spec, properties);
                log.info("[DDL] Successfully created Iceberg table: {}", tableIdentifier);
                GlobalSetConfInfo.IceBergCacheTableMap.put(setTableKeyName, iceBergTable);
            } else {
                if (Constant.dropTableFlag) {
                    log.warn("[DDL] DROP_TABLE_FLAG is true, dropping existing table: {}", tableIdentifier);
                    catalog.dropTable(tableIdentifier, true);
                    log.info("[DDL] Successfully dropped table: {}, proceeding to recreate it.", tableIdentifier);
                    properties.put("engine.hive.enabled", "true");
                    if (!pkNames.isEmpty() && "transaction".equals(Constant.icebergWriteMode)) {
                        properties.put(TableProperties.DELETE_MODE, "merge-on-read");
                        properties.put(TableProperties.UPDATE_MODE, "merge-on-read");
                        properties.put(TableProperties.MERGE_MODE,  "merge-on-read");
                        log.info("[DDL] table={} pk={} set merge-on-read", setTableKeyName, pkNames);
                    }
                    iceBergTable = catalog.createTable(tableIdentifier, GlobalSetConfInfo.IceBergSchemaCahceMap.get(setTableKeyName), spec, properties);
                    log.info("[DDL] Successfully recreated Iceberg table: {}", tableIdentifier);
                    GlobalSetConfInfo.IceBergCacheTableMap.put(setTableKeyName, iceBergTable);
                } else {
                    iceBergTable = catalog.loadTable(tableIdentifier);
                    log.info("[DDL] Successfully loaded existing Iceberg table: {}", tableIdentifier);
                    GlobalSetConfInfo.IceBergCacheTableMap.put(setTableKeyName, iceBergTable);
                    // load 场景：用 HMS 真实 schema 覆盖内存自建 schema，确保两者完全一致
                    GlobalSetConfInfo.IceBergSchemaCahceMap.put(setTableKeyName, iceBergTable.schema());
                }
            }
            for (org.apache.iceberg.types.Types.NestedField f : iceBergTable.schema().columns()) {
                GlobalSetConfInfo.columnTypeCache.put(setTableKeyName + "." + f.name(), f.type());
            }
            List<org.apache.iceberg.types.Types.NestedField> cols = iceBergTable.schema().columns();
            int colCount = cols.size();

            String[] colNames = new String[colCount];
            Map<String, Integer> colPosMap = new HashMap<>(colCount * 2);
            @SuppressWarnings("unchecked")
            java.util.function.Function<String, Object>[] converters =
                    new java.util.function.Function[colCount];

            for (int i = 0; i < colCount; i++) {
                org.apache.iceberg.types.Types.NestedField f = cols.get(i);
                String name = f.name(); // schema 里已经是 lowercase
                colNames[i] = name;
                colPosMap.put(name, i);
                converters[i] = buildConverter(f.type());
            }

            GlobalSetConfInfo.tableColumnNamesCache.put(setTableKeyName, colNames);
            GlobalSetConfInfo.tableColumnPosCache.put(setTableKeyName, colPosMap);
            GlobalSetConfInfo.tableColumnConvertersCache.put(setTableKeyName, converters);
            log.info("[DDL] column acceleration cache has been built. table={} cols={}", setTableKeyName, colCount);
            log.info("[DDL] columnTypeCache filled table={} cols={}", setTableKeyName, iceBergTable.schema().columns().size());

            socketReturnVo.setReturnFlag(true);
            socketReturnVo.setRowsNo(1);
            log.info("[DDL] Finished importHiveTable_IceBerg_Table for table: {}", setTableKeyName);
            return socketReturnVo;

        }catch (Exception e) {
            log.error("[DDL] importHiveTable_IceBerg_Table failed table={} err={}",
                    setTableKeyName, e.getMessage(), e);
            e.printStackTrace();
            socketReturnVo.setReturnFlag(false);
            socketReturnVo.setTradeType("kafkaTable");
            socketReturnVo.setErrorMsg(e.getMessage());
            return socketReturnVo;
        } finally{

        }

    }

    // =========================================================================
    // 类型映射辅助方法
    // =========================================================================

    /**
     * 将 Oracle colFlag 映射到 Iceberg 物理类型。
     * <p>
     * 映射优先级：
     *   1. 以客户提供的 Oracle→Iceberg 映射表为准。
     *   2. 映射表中标注为 "-" 或未出现的类型，统一降级为 StringType。
     * <p>
     * Oracle colFlag 对照（oraTypeConvert）：
     *   1=varchar2, 2=number, 8=long, 9=varchar, 12=date, 23=raw, 24=long_raw,
     *   69=rowid, 96=char, 100=binary_float, 101=binary_double, 106=mlslabel,
     *   111=lob, 112=clob, 113=blob, 114=bfile, 115=cfile, 121=object,
     *   123=collection, 178=time, 180=timestamp, 181=timestamp_tz,
     *   182=timestamp_ytm, 183=timestamp_dts, 208=urowid, 231=timestamp_ltz
     */
    private org.apache.iceberg.types.Type resolveIcebergType(TableColumnVo columnVo, String setTableKeyName) {
        System.out.printf("ColName: %-15s ,ColType: %-5s ,SourceType: %-5s, NumType: %-5s, isAdd: %-5s, cFlag: %-5s, cType: %-5s ,cLen: %-5s ,cPre: %-5s\n",
                columnVo.getColumnName(), columnVo.getColumnType(),
                columnVo.getSourceType() != null ? ConversionUtil.getIntFromBytes(columnVo.getSourceType()) : "null",
                columnVo.getNumberType(), columnVo.isAddColFlag(),
                columnVo.getcFlag() != null ? ConversionUtil.getShortFromBytes(columnVo.getcFlag(), false) : "null",
                columnVo.getcType() != null ? ConversionUtil.getIntFromBytes(columnVo.getcType()) : "null",
                columnVo.getColumnLen() != null ? Integer.parseInt(columnVo.getColumnLen()) : "null",
                columnVo.getColPrecision());
         switch (columnVo.getColumnType()) {

            // ===== 客户映射表：string ← char / varchar2 / clob / nclob =====
            case 1:   // varchar2
            case 9:   // varchar
            case 8:   // long（Oracle 字符类型，非数值 long）
            case 96:  // char
            case 112: // clob / nclob
                // ===== 不在映射表，降级 string =====
            case 5007: // 内部自定义类型
            case 69:   // rowid
            case 208:  // urowid
            case 106:  // mlslabel（已废弃）
            case 121:  // object
            case 123:  // collection
            case 178:  // time（Oracle 无原生 TIME 类型，降级 string）
            case 182:  // INTERVAL YEAR TO MONTH
            case 183:  // INTERVAL DAY TO SECOND
            case 24:   // long raw（不在映射表，降级 string）
            case 111:  // lob（不在映射表，降级 string）
            case 114:  // bfile（不在映射表，降级 string）
            case 115:  // cfile（不在映射表，降级 string）
                // ===== 二进制类统一按 string 处理 =====
            case 23:   // RAW
            case 113:  // BLOB
                return Types.StringType.get();

            // ===== 客户映射表：timestamp ← date / timestamp =====
            case 12:   // Oracle DATE（含时分秒）
                return Types.DateType.get();
            case 180:  // TIMESTAMP
                // 长度为 1 时，源端实际是 TIME 类型，按 Iceberg TimeType 建表
                if (Integer.parseInt(columnVo.getColumnLen())==1) {
                    return Types.TimeType.get();
                }
                return Types.TimestampType.withoutZone();

            // ===== 客户映射表：timestamptz ← timestamp with time zone =====
            case 231:
            case 181:  // TIMESTAMP WITH TIME ZONE
                return Types.TimestampType.withZone();

            case 100:  // BINARY_FLOAT → FloatType，与 Oracle 原始类型语义完全对应
                return Types.FloatType.get();

            case 101:  // BINARY_DOUBLE → DoubleType，与 Oracle 原始类型语义完全对应
                return Types.DoubleType.get();

            // ===== NUMBER 精度二次判断（见 resolveNumberType）=====
            case 2:
                return resolveNumberType(columnVo, setTableKeyName);

            default:
                log.info("[DDL] unknown columnType={} col={}, fallback to StringType",
                        columnVo.getColumnType(), columnVo.getColumnName());
                return Types.StringType.get();
        }
    }

    /**
     * Oracle NUMBER 精度映射。
     * <p>
     * 用户确认：所有 NUMBER(n,0) 不论 n 是几，numberType 一律为 1100。
     * <p>
     * numberType 分类：
     *   1000 = NUMBER（无任何精度标注）       → long（客户要求）
     *   1100 = NUMBER(n,0)                  → boolean(n=1) / long(n≤18) / decimal(n>18)
     *   1200 = NUMBER(%d)（只有精度无标度）   → decimal(p,0)
     *   3000 = NUMBER(%d,%d)               → decimal(p,s)
     *   3100 = FLOAT(%d)                   → decimal(p,10)
     */
    private org.apache.iceberg.types.Type resolveNumberType(TableColumnVo columnVo, String setTableKeyName) {
        String colKey = setTableKeyName + "." + columnVo.getColumnName().toLowerCase();
        int numberType = columnVo.getNumberType();
        // 注册 numberType，写入层 convertValue 会用到
        GlobalConfCommInfo.jddmEngineTypeByNumberColMap.put(colKey, numberType);

        switch (numberType) {

            case 1000: { // NUMBER（无任何精度标注）→ long（客户要求）
                return Types.LongType.get();
            }

            case 1100: { // NUMBER(n,0) — 用户确认所有 number(n,0) 都走这里
                int len = safeParseInt(columnVo.getColumnLen(), 18);
                if (len == 1)        return Types.BooleanType.get();              // NUMBER(1,0) → boolean
                else if (len <= 18)  return Types.LongType.get();                // NUMBER(2~18,0) → long
                else                 return Types.DecimalType.of(Math.min(len, 38), 0); // NUMBER(19+,0) → decimal
            }

            case 1200: { // NUMBER(%d)（只有精度无标度）
                int p = safeParseInt(columnVo.getColumnLen(), 18);
                if (p > 38) return Types.StringType.get(); // 超出 decimal 上限，降级 string
                return Types.DecimalType.of(p, 0);
            }

            case 3000: { // NUMBER(%d,%d)
                int p = safeParseInt(columnVo.getColumnLen(), 18);
                int s = safeParseInt(columnVo.getColPrecision(), 0);
                if (p > 38) p = 38;
                // 注册 scale，写入层 convertValue 校验精度用
                GlobalSetConfInfo.jddmEngineTypeByNumberColMap.put(colKey, s);
                return Types.DecimalType.of(p, s);
            }

            case 3100: { // FLOAT(%d) → decimal(p,10)
                int fp = safeParseInt(columnVo.getColumnLen(), 38);
                if (fp > 38) return Types.StringType.get();
                return Types.DecimalType.of(fp, 10);
            }

            default:
                return Types.DecimalType.of(38, 18);
        }
    }

    /**
     * 安全地将字符串转为 int，解析失败时返回 defaultVal。
     */
    private static int safeParseInt(String s, int defaultVal) {
        if (s == null || s.trim().isEmpty()) return defaultVal;
        try { return Integer.parseInt(s.trim()); } catch (NumberFormatException e) { return defaultVal; }
    }
    /**
     * 根据 Iceberg 字段类型，预先生成对应的转换函数。
     * 这样热路径只需 converters[i].apply(rawValue)，
     * 彻底消除每列每行的 instanceof 链判断。
     */
    @SuppressWarnings("unchecked")
/**
 * 根据 Iceberg 字段类型预构建转换函数。
 * 放在本类中，直接复用类内已有的 DateTimeFormatter 和 parseTimestampValue 方法，
 * 避免跨类引用的编译问题。
 */
    public static java.util.function.Function<String, Object> buildConverter(
            org.apache.iceberg.types.Type fieldType) {

        if (fieldType instanceof org.apache.iceberg.types.Types.StringType) {
            return (String v) -> v;

        } else if (fieldType instanceof org.apache.iceberg.types.Types.BooleanType) {
            return (String v) -> "1".equals(v.trim())
                    || "true".equalsIgnoreCase(v.trim())
                    || "t".equalsIgnoreCase(v.trim());

        } else if (fieldType instanceof org.apache.iceberg.types.Types.LongType) {
            return (String v) -> {
                String t = v.trim();
                try { return Long.parseLong(t); }
                catch (NumberFormatException e1) {
                    try { return new java.math.BigDecimal(t).longValueExact(); }
                    catch (Exception e2) {
                        int dot = t.indexOf('.');
                        return Long.parseLong(dot >= 0 ? t.substring(0, dot) : t);
                    }
                }
            };

        } else if (fieldType instanceof org.apache.iceberg.types.Types.IntegerType) {
            return (String v) -> Integer.parseInt(v.trim());

        } else if (fieldType instanceof org.apache.iceberg.types.Types.DecimalType) {
            org.apache.iceberg.types.Types.DecimalType dt =
                    (org.apache.iceberg.types.Types.DecimalType) fieldType;
            int scale = dt.scale();
            return (String v) -> new java.math.BigDecimal(v.trim())
                    .setScale(scale, java.math.RoundingMode.HALF_UP);

        } else if (fieldType instanceof org.apache.iceberg.types.Types.FloatType) {
            return (String v) -> Float.parseFloat(v.trim());

        } else if (fieldType instanceof org.apache.iceberg.types.Types.DoubleType) {
            return (String v) -> Double.parseDouble(v.trim());

        } else if (fieldType instanceof org.apache.iceberg.types.Types.DateType) {
            return (String v) -> {
                String t = v.trim();
                if (t.length() > 10 && t.charAt(10) == ' ') t = t.substring(0, 10);
                try { return java.time.LocalDate.parse(t, FMT_DATE); }       catch (Exception ignored) {}
                try { return java.time.LocalDate.parse(t, FMT_DATE_SLASH); } catch (Exception ignored) {}
                return null;
            };

        } else if (fieldType instanceof org.apache.iceberg.types.Types.TimeType) {
        return (String v) -> {
            String t = v.trim();
            int spaceIdx = t.indexOf(' ');
            if (spaceIdx >= 0) t = t.substring(spaceIdx + 1);
            try { return java.time.LocalTime.parse(t, java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS")); } catch (Exception ignored) {}
            try { return java.time.LocalTime.parse(t, java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss.SSS")); } catch (Exception ignored) {}
            try { return java.time.LocalTime.parse(t); } catch (Exception ignored) {}
            return null;
        };

    } else if (fieldType instanceof org.apache.iceberg.types.Types.TimestampType) {
        boolean withZone = ((org.apache.iceberg.types.Types.TimestampType) fieldType).shouldAdjustToUTC();
        // 跨类调用，parseTimestampValue 改为 static 后可以直接引用
        return (String v) -> OperationTotalSyncByIceBergThreadPool.parseTimestampValue(v.trim(), withZone);

    } else {
        return (String v) -> v;
    }
    }
}