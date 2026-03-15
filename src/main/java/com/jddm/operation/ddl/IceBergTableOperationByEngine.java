package com.jddm.operation.ddl;

import com.dsg.analysis.tableInfo.vo.TableColumnVo;
import com.dsg.analysis.tableInfo.vo.TableInfoVo;
import com.dsg.analysis.utils.ConversionUtil;
import com.dsg.operation.common.ConstantSet;
import com.jddm.common.Constant;
import com.jddm.common.ConstantColType;
import com.jddm.conf.GlobalConfInfo;
import com.jddm.conf.GlobalSetConfInfo;
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

public class IceBergTableOperationByEngine {

//	private static Logger log = LogManager.getLogger(IceBergTableOperationByEngine.class);
	public Logger log = LogManager.getLogger(IceBergTableOperationByEngine.class);
	
	
	/*** *** *** ***
	* @functionName（方法名称）: importHiveTable_IceBerg_Table 
	* @description （方法说明）: Create Iceberg table in Hive DB | 在Hive库中创建Iceberg表；
	* @param       （传入参数）: tableInfoVo     --->  Type:: TableInfoVo
	* @param       （传入参数）: thisTableColMap --->  Type:: Map<String,String> 
	* @return      （返回）   :  SocketReturnVo
	* @exception   （异常）   : Exception ex
	* @author      （创建人）: JH
	* @since       （创建时间）:  2023/06/29 AM 0:55
	* @ModifyTime  （修改时间）:  
	***/
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
	    	log.info(" ICEBerg Schema ::"+tableInfoVo.getOwner().toLowerCase()+" TName ::"+tableInfoVo.getTableName().toLowerCase()+" tableSpace ::"+tableInfoVo.getTableSpace());
	    }


		List<Integer> pkColumnList = tableInfoVo.getPkColumnNo();
		//StringBuffer pkStringInfo = new StringBuffer();
		Map<String,String> pkColumnMap=new HashMap<String,String>();
		//pkStringInfo.append("ALTER TABLE TEST_TAB ADD CONSTRAINT TAST_PK PRIMARY KEY(");
		for( TableColumnVo columnVo: columnList){
			
			if(thisTableColMap.containsKey(columnVo.getColumnName().toLowerCase())){
				thisTableColMap.remove(columnVo.getColumnName().toLowerCase());
			}
			
			if(pkColumnList !=null && !pkColumnList.isEmpty()){
				
				for(Integer pkNo :pkColumnList){
					
					if(pkNo == columnVo.getColumnNo()){
                        log.info("Table: {} ,PrimaryKeyName: {} ,pkNo: {}",setTableKeyName,columnVo.getColumnName().toLowerCase(),pkNo);
						pkColumnMap.put(columnVo.getColumnName().toLowerCase(), pkNo+"");
						break;
					}
				}
			}
		}
        List<String> pkNames = new ArrayList<>(pkColumnMap.keySet());
        GlobalSetConfInfo.TablePkColCacheMap.put(setTableKeyName, pkNames);
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
				
				log.info(" List_Size ::"+columnList.size()+" ################ ADD COLUMN ::"+addColMap.getKey()+" Type ::"+addColMap.getValue());
				
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
				log.info(" --dict[ALL FieldType]-- >>> "+columnVo.getColumnName().toLowerCase()+" value ::"+columnVo.getColumnType()+" NumberType ::"+columnVo.getNumberType()+" columnNo ::"+columnVo.getColumnNo()+" commons ::"+columnVo.getColComment()+" ("+columnVo.getColumnLen()+","+columnVo.getColPrecision()+")");

			}


            if(columnVo.isAddColFlag()) {
                if(ConstantPublic.setPartitionFlag){
                    if(columnVo.getColumnName().toLowerCase().equalsIgnoreCase(ConstantPublic.setTablePartitionColName)){
                        log.info(" JddmEngine ######## Partition COLUMN ::"+columnVo.getColumnName().toLowerCase()+" Type ::"+columnVo.getColumnType());
                        yloaderColumnList.add(columnVo);
                        continue;
                    }else {
                        log.info(" JddmEngine ######## Partition ADD COLUMN ::"+columnVo.getColumnName().toLowerCase()+" Type ::"+columnVo.getColumnType());
                    }
                }else {
                    log.info(" JddmEngine ################ ADD COLUMN ::"+columnVo.getColumnName().toLowerCase()+" Type ::"+columnVo.getColumnType());
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
		
		log.info(" JDDM_ICEBERG_HIVE_Engine List_Size ::"+columnList.size()+" operationList ::"+operColumnList.size());
	    
		if(Constant.settingDataBaseName !=null && !Constant.settingDataBaseName.equals("")) {
			hiveExtTableName = Constant.settingDataBaseName+"."+FileUtils.createTableName_ByJddmEngine(tableInfoVo.getOwner().toLowerCase(),tableInfoVo.getTableName().toLowerCase());
		}else {
			hiveExtTableName = tableInfoVo.getOwner().toLowerCase()+"."+FileUtils.createTableName_ByJddmEngine(tableInfoVo.getOwner().toLowerCase(),tableInfoVo.getTableName().toLowerCase());
		}
		
		
		log.info(" JDDM_ICEBERG_HIVE_Engine hiveTable::["+hiveExtTableName+"] List_Size ::"+columnList.size()+" operationList ::"+operColumnList.size());
		
		if(ConstantPublic.setPartitionFlag){
			insertSelectString.append("INSERT INTO "+hiveExtTableName+ " partition("+ConstantPublic.setTablePartitionColName+"=@dsg_date@) ( ");
		}else{
			insertSelectString.append("INSERT INTO "+hiveExtTableName+ " partition(busi_date=@dsg_date@) ( ");
		}
		
		insertSqlString.append("INSERT INTO "+hiveExtTableName+ " VALUES (");
		//insertSqlString.append("INSERT INTO "+maxComputerTableName+ "  VALUES (");
		createSqlString.append("create table IF NOT EXISTS "+hiveExtTableName+" ( ");
		
		GlobalConfInfo.jddmEngineByHiveTableCacheMap.put(tableInfoVo.getOwner().toLowerCase()+"."+tableInfoVo.getTableName().toLowerCase(), hiveExtTableName);
		
		
		List<Types.NestedField> iceBergTablefields = new ArrayList<>();
		
		
		
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
				log.info(" --reload[ALL FieldType]-- >>> "+columnVo.getColumnName().toLowerCase()+" value ::"+columnVo.getColumnType()+" NumberType ::"+columnVo.getNumberType()+" columnNo ::"+columnVo.getColumnNo()+" commons ::"+columnVo.getColComment()+" ("+columnVo.getColumnLen()+","+columnVo.getColPrecision()+")");

			}
            boolean isTransactionMode = "transaction".equals(Constant.icebergWriteMode);
            boolean isPkCol = isTransactionMode && pkColumnMap.containsKey(columnVo.getColumnName().toLowerCase());
			switch(columnVo.getColumnType()){

				case 1:
				case 9:
                case 8:
                case 5007:
                case 231:
                case 182: //INTERVAL YEAR(2) TO MONTH
                case 183: //INTERVAL DAY(2) TO SECOND(6)
				case 12: //0x0c ---> date (2012-12-12 12:12:12)
                case 23: // 0x17 --> raw
                case 24: //0x18 ---> longRaw
                case 69: //0x45 --> rowid
                case 96: //0x60 ---> char
                    //log.info(" --reload[FieldType.TIMESTAMP]-- >>> "+columnVo.getColumnName().toLowerCase()+" value ::"+columnVo.getColumnType()+" columnNo ::"+columnVo.getColumnNo());
                    //columnSqlString.append(columnVo.getColumnName().toLowerCase()+" datetime,");
                    //columnSqlString.append("`"+columnVo.getColumnName().toLowerCase()+"` timestamp,"); //@@@@@@@@@@@@@@@@@@@@@@@@@@@
                    //log.info(" --reload[FieldType.CHAR]-- >>> "+columnVo.getColumnName().toLowerCase()+" value ::"+columnVo.getColumnType()+" columnNo ::"+columnVo.getColumnNo());

                    //columnSqlString.append(columnVo.getColumnName().toLowerCase()+" datetime,");  //@@@@@@@@@@@@@@@@@@@@@@@@@@@
                    //log.info(" --reload(PG_varchar)->[FieldType.STRING]-- >>> "+columnVo.getColumnName().toLowerCase()+" value ::"+columnVo.getColumnType()+" columnNo ::"+columnVo.getColumnNo());
                    //Objects.requireNonNull(Types.StringType.get())
                    //log.info(" --reload[FieldType.STRING]-- >>> "+columnVo.getColumnName().toLowerCase()+" value ::"+columnVo.getColumnType()+" columnNo ::"+columnVo.getColumnNo());

                    nestedField = isPkCol
                            ? Types.NestedField.required(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.StringType.get())
                            : Types.NestedField.optional(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.StringType.get());
                    iceBergTablefields.add(nestedField);
					
					break;
                case 2:
						
					log.info(" --reload[FieldType.BIGINT]-- >>> "+columnVo.getColumnName().toLowerCase()+" value ::"+columnVo.getColumnType()+" NumberType ::"+columnVo.getNumberType()+" columnNo ::"+columnVo.getColumnNo()+" commons ::"+columnVo.getColComment()+" ("+columnVo.getColumnLen()+","+columnVo.getColPrecision()+")");
					
					switch(columnVo.getNumberType()) {
						case 1000: //NUMBER
							
							GlobalConfCommInfo.jddmEngineTypeByNumberColMap.put(setTableKeyName+"."+columnVo.getColumnName().toLowerCase(), 1000);
//							nestedField = Types.NestedField.optional(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.DecimalType.of(38, 18));
                            nestedField = isPkCol
                                    ? Types.NestedField.required(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.StringType.get())
                                    : Types.NestedField.optional(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.StringType.get());
                            iceBergTablefields.add(nestedField);

							break;
						case 1100: //NUMBER(*, 0)
							GlobalConfCommInfo.jddmEngineTypeByNumberColMap.put(setTableKeyName+"."+columnVo.getColumnName().toLowerCase(), 1100);
//							nestedField = Types.NestedField.optional(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.DecimalType.of(Integer.parseInt(columnVo.getColumnLen()), 0));
                            nestedField = isPkCol
                                    ? Types.NestedField.required(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.StringType.get())
                                    : Types.NestedField.optional(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.StringType.get());
                            iceBergTablefields.add(nestedField);
							break;
						case 1200: //NUMBER(%d)
							GlobalConfCommInfo.jddmEngineTypeByNumberColMap.put(setTableKeyName+"."+columnVo.getColumnName().toLowerCase(), 1200);
							if (Integer.parseInt(columnVo.getColumnLen()) > 38) {
//			                    nestedField = Types.NestedField.optional(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.DecimalType.of(38, 0));
                                nestedField = isPkCol
                                        ? Types.NestedField.required(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.StringType.get())
                                        : Types.NestedField.optional(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.StringType.get());
                                iceBergTablefields.add(nestedField);
			                } else {
//			                    nestedField = Types.NestedField.optional(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.DecimalType.of(Integer.parseInt(columnVo.getColumnLen()), 0));
                                nestedField = isPkCol
                                        ? Types.NestedField.required(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.StringType.get())
                                        : Types.NestedField.optional(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.StringType.get());
                                iceBergTablefields.add(nestedField);
			                }
							break;
						case 3000: //NUMBER(%d,%d)
							GlobalConfCommInfo.jddmEngineTypeByNumberColMap.put(setTableKeyName+"."+columnVo.getColumnName().toLowerCase(), 3000);
							GlobalSetConfInfo.jddmEngineTypeByNumberColMap.put(setTableKeyName+"."+columnVo.getColumnName().toLowerCase(), Integer.parseInt(columnVo.getColPrecision()));
//							nestedField = Types.NestedField.optional(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.DecimalType.of(Integer.parseInt(columnVo.getColumnLen()), Integer.parseInt(columnVo.getColPrecision())));
                            nestedField = isPkCol
                                    ? Types.NestedField.required(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.StringType.get())
                                    : Types.NestedField.optional(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.StringType.get());

							iceBergTablefields.add(nestedField);
							break;
						case 3100: //FLOAT(%d) |DOUBLE(%d)
							GlobalConfCommInfo.jddmEngineTypeByNumberColMap.put(setTableKeyName+"."+columnVo.getColumnName().toLowerCase(), 3100);
							if (Integer.parseInt(columnVo.getColumnLen()) > 38) {
//			                    nestedField = Types.NestedField.optional(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.DecimalType.of(38, 0));
                                nestedField = isPkCol
                                        ? Types.NestedField.required(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.StringType.get())
                                        : Types.NestedField.optional(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.StringType.get());
                                iceBergTablefields.add(nestedField);
			                } else {
//			                    nestedField = Types.NestedField.optional(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.DecimalType.of(Integer.parseInt(columnVo.getColumnLen()), 0));
                                nestedField = isPkCol
                                        ? Types.NestedField.required(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.StringType.get())
                                        : Types.NestedField.optional(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.StringType.get());
                                iceBergTablefields.add(nestedField);
			                }
							break;
						default:
//							nestedField = Types.NestedField.optional(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.DecimalType.of(38, 18));
                            nestedField = isPkCol
                                    ? Types.NestedField.required(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.StringType.get())
                                    : Types.NestedField.optional(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.StringType.get());
                            iceBergTablefields.add(nestedField);
							break;
					}
					
					insertSqlString.append("?,");
					break;
				case 181: //0xb5 --->TIMESTAMP 2012-12-12 12:12:12.123456789 +时区
                case 180: //0xb4 --->TIMESTAMP 2012-12-12 12:12:12.123456789
                    nestedField = isPkCol
                            ? Types.NestedField.required(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.StringType.get())
                            : Types.NestedField.optional(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.StringType.get());
                    iceBergTablefields.add(nestedField);
					break;

				case 100: //BINARY_FLOAT
				case 101: //BINARY_DOUBLE
                    nestedField = isPkCol
                            ? Types.NestedField.required(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.StringType.get())
                            : Types.NestedField.optional(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.StringType.get());
                    iceBergTablefields.add(nestedField);
					break;
				case 112: //CLOB 0x70
                case 113: //BLOB 0x71
/*					nestedField = Types.NestedField.optional(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.StringType.get());
					iceBergTablefields.add(nestedField);*/
					switch(ConversionUtil.getIntFromBytes(columnVo.getSourceType())) {
						case 5009: // bytes
							GlobalConfCommInfo.jddmEngineTypeBy0x71BytesColMap.put(setTableKeyName+"."+columnVo.getColumnName().toLowerCase(),columnVo.getColumnName().toLowerCase());
                            nestedField = isPkCol
                                    ? Types.NestedField.required(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.StringType.get())
                                    : Types.NestedField.optional(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.StringType.get());
                            iceBergTablefields.add(nestedField);
							break;
						case 5019: //uuid
                            nestedField = isPkCol
                                    ? Types.NestedField.required(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.StringType.get())
                                    : Types.NestedField.optional(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.StringType.get());
                            iceBergTablefields.add(nestedField);
							break;
						default:
                            nestedField = isPkCol
                                    ? Types.NestedField.required(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.StringType.get())
                                    : Types.NestedField.optional(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.StringType.get());
                            iceBergTablefields.add(nestedField);
							break;
					}


					break;


                case 5001: //pg database int
						//log.info(" --reload( PG_int )->[FieldType.BIGINT]-- >>> "+columnVo.getColumnName().toLowerCase()+" value ::"+columnVo.getColumnType()+" columnNo ::"+columnVo.getColumnNo());
                    nestedField = isPkCol
                            ? Types.NestedField.required(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.StringType.get())
                            : Types.NestedField.optional(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.StringType.get());
                    iceBergTablefields.add(nestedField);

                    break;
                default:
						//log.info(" columnType ::"+columnVo.getColumnType());
					log.info(" --reload columnType["+columnVo.getColumnType()+"]-- >>> "+columnVo.getColumnName().toLowerCase()+" value ::"+columnVo.getColumnType()+" columnNo ::"+columnVo.getColumnNo());
					break;
			}
		}
		
		for(TableColumnVo columnVo: yloaderColumnList) {
			Types.NestedField nestedField = null;
            boolean isTransactionMode = "transaction".equals(Constant.icebergWriteMode);
            boolean isPkCol = isTransactionMode && pkColumnMap.containsKey(columnVo.getColumnName().toLowerCase());
			GlobalConfCommInfo.jddmEngineTypeByYloaderColMap.put(setTableKeyName+"."+columnVo.getColumnName().toLowerCase(),columnVo.getColumnName().toLowerCase());
            nestedField = isPkCol
                    ? Types.NestedField.required(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.StringType.get())
                    : Types.NestedField.optional(iceBergTablefields.size() + 1, columnVo.getColumnName().toLowerCase(), Types.StringType.get());
            iceBergTablefields.add(nestedField);
		}
		
		iceBergSchema = new Schema(iceBergTablefields);
		log.info("------>>> Create Schema Sql::"+iceBergSchema.toString());

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
			hadoopConf.set("fs.defaultFS", Constant.fsDefaultInfo); // 确保强制生效
			catalog.setConf(hadoopConf);
            Map<String, String> properties = new HashMap<String, String>();
            properties.put(CatalogProperties.WAREHOUSE_LOCATION, Constant.fsDefaultInfo);
            properties.put(CatalogProperties.URI, Constant.hiveMetastoreUris);
            properties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.hive.HiveCatalog");
            properties.put("format-version", "2");
// 有主键的表开启 merge-on-read，支持 equality delete

            catalog.initialize("hive", properties);
            log.info("------>>> Original config fsDefaultInfo : {} ,hiveMetastoreUris : {}", Constant.fsDefaultInfo, Constant.hiveMetastoreUris);
/*            StringBuilder confSb = new StringBuilder();
            for (Map.Entry<String, String> entry : catalog.getConf()) {
                if (entry.getKey().startsWith("fs.") || entry.getKey().startsWith("dfs.") || entry.getKey().startsWith("hive.") || entry.getKey().startsWith("hadoop.")) {
                    confSb.append(entry.getKey()).append("=").append(entry.getValue()).append("\n");
                }
            }*/
//            log.info("------>>> Read from HiveCatalog Conf (fs/dfs/hive/hadoop) :\n{}", confSb.toString());
            log.info("------>>> HiveCatalog Initialize Properties : {}", properties);

// PartitionSpec：有分区字段用 identity，否则无分区
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

            //如果库不存在，则创建它
            if (!catalog.namespaceExists(ns)) {
                catalog.createNamespace(ns);
                log.info("Created missing namespace: {}", ns);
            }
			log.info("------>>> Create Table Sql:: "+tableIdentifier);
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
					// 其它未知异常，直接抛出
					throw e;
				}
			}

			if(!isTableExists) {
				properties.put("engine.hive.enabled", "true");
/*				String forceLocation = Constant.fsDefaultInfo+"/data/warehouse/tablespace/managed/hive/"
				                        + tableInfoVo.getOwner().toLowerCase() + ".db/"
				                        + tableInfoVo.getTableName().toLowerCase();
				 properties.put(TableProperties.WRITE_DATA_LOCATION, forceLocation + "/data");
				 properties.put(TableProperties.WRITE_METADATA_LOCATION, forceLocation + "/metadata");
				 properties.put("location", forceLocation);
				 log.info("------>>> Force specified table location: {}", forceLocation);*/
                if (!pkNames.isEmpty() && "transaction".equals(Constant.icebergWriteMode)) {
                    properties.put(TableProperties.DELETE_MODE, "merge-on-read");
                    properties.put(TableProperties.UPDATE_MODE, "merge-on-read");
                    properties.put(TableProperties.MERGE_MODE,  "merge-on-read");
                    log.info("[DDL] table={} pk={} set merge-on-read", setTableKeyName, pkNames);
                }
				iceBergTable = catalog.createTable(tableIdentifier, GlobalSetConfInfo.IceBergSchemaCahceMap.get(setTableKeyName),spec,properties);
				GlobalSetConfInfo.IceBergCacheTableMap.put(setTableKeyName, iceBergTable);
			}else {
				if(Constant.dropTableFlag){
					log.warn("[DDL] DROP_TABLE_FLAG is true, dropping existing table: {}", tableIdentifier);
					catalog.dropTable(tableIdentifier,true);
					log.info("[DDL] Successfully dropped table: {}, proceeding to recreate it.", tableIdentifier);
					properties.put("engine.hive.enabled", "true");
                    if (!pkNames.isEmpty() && "transaction".equals(Constant.icebergWriteMode)) {
                        properties.put(TableProperties.DELETE_MODE, "merge-on-read");
                        properties.put(TableProperties.UPDATE_MODE, "merge-on-read");
                        properties.put(TableProperties.MERGE_MODE,  "merge-on-read");
                        log.info("[DDL] table={} pk={} set merge-on-read", setTableKeyName, pkNames);
                    }
					iceBergTable = catalog.createTable(tableIdentifier, GlobalSetConfInfo.IceBergSchemaCahceMap.get(setTableKeyName),spec,properties);
					GlobalSetConfInfo.IceBergCacheTableMap.put(setTableKeyName, iceBergTable);
				}else {
					iceBergTable = catalog.loadTable(tableIdentifier);
					
					GlobalSetConfInfo.IceBergCacheTableMap.put(setTableKeyName, iceBergTable);
				}
			}

			socketReturnVo.setReturnFlag(true);
			socketReturnVo.setRowsNo(1);
			socketReturnVo.setTradeType("kafkaTable");
			
			return socketReturnVo;
		    
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

			socketReturnVo.setReturnFlag(false);
			socketReturnVo.setTradeType("kafkaTable");
			socketReturnVo.setErrorMsg(e.getMessage());
			return socketReturnVo;
		} finally{
			
		}
	    
	}
}
