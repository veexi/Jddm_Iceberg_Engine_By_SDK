package com.jddm.operation.timer;

import com.dsg.analysis.tableInfo.vo.TableColumnVo;
import com.dsg.analysis.tableInfo.vo.TableInfoVo;
import com.dsg.analysis.utils.ConversionUtil;
import com.jddm.common.Constant;
import com.jddm.conf.GlobalConfInfo;
import com.jddm.operation.ddl.IceBergTableOperationByEngine;
import com.publics.cache.TableAllCacheInfo;
import com.publics.common.ConstantPublic;
import com.publics.conf.GlobalConfCommInfo;
import com.publics.operation.jdbcOper.embeddedDB.SQLiteJDBC;
import com.publics.utils.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.*;

public class ReLoaderTableInfoBySqliteDB implements Runnable{
 	public Logger log = LogManager.getLogger(ReLoaderTableInfoBySqliteDB.class);
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		byte[] tableInfoArr = null;
		byte[] sourceTableInfoArr = null;
		SQLiteJDBC sqliteDBOperation = null;
		TableAllCacheInfo tableCacheInfo = null;
		try {
			
			if(!GlobalConfCommInfo.ddlOperCacheTableKeyMap.isEmpty()) {
				GlobalConfInfo.reloadTableVoCalcMap.get(Constant.reloadKeyName).getAndIncrement();
						if(GlobalConfInfo.reloadTableVoCalcMap.get(Constant.reloadKeyName).get()>=3) {

							for(Map.Entry<String, String> reloadTableKeyVo:GlobalConfCommInfo.ddlOperCacheTableKeyMap.entrySet()){
								log.info("  JddmEngine[Hive&Hdfs]-ICEBERG ReLoading DDL ::"+reloadTableKeyVo.getKey().toLowerCase()+" Type ::"+reloadTableKeyVo.getValue());
								sqliteDBOperation = new SQLiteJDBC();
								tableInfoArr = sqliteDBOperation.query_TableContentObject_ByKey(reloadTableKeyVo.getKey().toLowerCase());
								sourceTableInfoArr = sqliteDBOperation.query_TableContentObject_ByKey(reloadTableKeyVo.getKey().toLowerCase()+"_source");
								if(tableInfoArr !=null && tableInfoArr.length>3) {
									tableCacheInfo = new TableAllCacheInfo();
									//加载yloader字典
									TableInfoVo tableInfoDBVo = tableCacheInfo.serializableTableVo_ToByteArray(tableInfoArr);

									TableInfoVo sourceTableInfoDBVo =tableCacheInfo.serializableTableVo_ToByteArray(sourceTableInfoArr);

									GlobalConfCommInfo.cacheSourceTableInfoMap.put(reloadTableKeyVo.getKey().toLowerCase(), sourceTableInfoDBVo);

									log.info("  [ICEBERG] Query Yloader Table ColumnList Size ::"+tableInfoDBVo.getColumnList().size());
									tableCacheInfo.mergeSourceAndYloaderDictionary(reloadTableKeyVo.getKey().toLowerCase(),tableInfoDBVo);

									//删除原表缓存；
									GlobalConfCommInfo.cacheSourceTableInfoMap.remove(reloadTableKeyVo.getKey().toLowerCase());

									for(TableColumnVo colSyncVo:(List<TableColumnVo>)tableInfoDBVo.getColumnList()) {

										if(colSyncVo.getSourceType()!=null && colSyncVo.getcFlag() !=null) {
											log.info(String.format(" JDDM(Hive&Hdfs)Engine reLoading DDL S.T::%20s --> %10s Type ::%3d SType ::%8s CFlag:%s", reloadTableKeyVo.getKey().toLowerCase(),colSyncVo.getColumnName(),colSyncVo.getColumnType(),ConversionUtil.printHexString(colSyncVo.getSourceType()),ConversionUtil.printHexString(colSyncVo.getcFlag())));
										}else {
											log.info(String.format(" JDDM(Hive&Hdfs)Engine reLoading DDL S.T::%20s --> %10s Type ::%3d SType ::%8s CFlag:%s", reloadTableKeyVo.getKey().toLowerCase(),colSyncVo.getColumnName(),colSyncVo.getColumnType(),colSyncVo.getSourceType(),colSyncVo.getcFlag()));
										}

									}
									log.info(" =================================================> [ICEBERG] End   ReLoading JddmEngine Table Partition Session .... ");

									String thisOdpsKeyName=tableInfoDBVo.getOwner().toLowerCase()+"."+tableInfoDBVo.getTableName().toLowerCase();
									GlobalConfCommInfo.cacheTableInfoMap.put(reloadTableKeyVo.getKey().toLowerCase(), tableInfoDBVo);

									log.info(" JddmEngine Put "+thisOdpsKeyName+" to JVM MemoryCache ... ... ");
									log.info(" =================================================> [ICEBERG] Loaded To JddmEngine MemoryCache  Complete !!! ");


                                    GlobalConfInfo.jddmEngineByHiveTableCacheMap.put(reloadTableKeyVo.getKey().toLowerCase(), tableInfoDBVo.getOwner().toLowerCase()+"."+FileUtils.createTableName_ByJddmEngine(tableInfoDBVo.getOwner(),tableInfoDBVo.getTableName()));

									if(!GlobalConfCommInfo.cacheTableInfoMap.get(reloadTableKeyVo.getKey().toLowerCase()).getColumnList().isEmpty()){
										TableColumnVo tableColumnVo = null;
										LinkedHashMap<String, String> tableColumnMap = new LinkedHashMap<String, String>();
										IceBergTableOperationByEngine iceBergTableOperationByEngine =  new IceBergTableOperationByEngine();
										for( Map.Entry<String, TableInfoVo> entry:GlobalConfCommInfo.cacheTableInfoMap.entrySet()) {
											String key = entry.getKey();
											TableInfoVo tableInfoVo = entry.getValue();
											iceBergTableOperationByEngine.importHiveTable_IceBerg_Table(tableInfoVo, tableColumnMap);
										}


										break;
									}


								}


							}
							GlobalConfInfo.reloadTableVoCalcMap.get(Constant.reloadKeyName).set(0);
							GlobalConfCommInfo.ddlOperCacheTableKeyMap.clear();
						} else {

							for(Map.Entry<String, String> reloadTableKeyVo:GlobalConfCommInfo.ddlOperCacheTableKeyMap.entrySet()) {

								log.info(" Jddm(Hive&Hdfs)Engine Reloaded DDL S.T ---> "+reloadTableKeyVo.getKey().toLowerCase()+" CalcNo :::["+GlobalConfInfo.reloadTableVoCalcMap.get(Constant.reloadKeyName).get()+"=3] ...");
							}
						}

				
			}
			
		}catch(Exception ex) {
			ex.printStackTrace();
		}finally {
		}
		
		
	}
	
	
	
}
