package com.jddm.thread;

import com.dsg.analysis.utils.ConversionUtil;
import com.dsg.analysis.vo.PackageReturnRowVo;
import com.dsg.analysis.vo.PackageReturnVo;
import com.dsg.analysis.vo.Udb_BcolumnVo;
import com.jddm.common.Constant;
import com.jddm.conf.GlobalConfInfo;
import com.jddm.conf.GlobalSetConfInfo;
import com.publics.common.ConstantPubSet;
import com.publics.common.ConstantPublic;
import com.publics.conf.GlobalConfCommInfo;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/*** *** ***
 * @projectName（项目名称）:  jddmGeneralDataEngine.jar
 * @package（包）: com.jddm.engine.thread
 * @className（类名称）: OperationTotalSyncByIceBergThreadPool ; @classType : Runnable Class
 * @description（类描述）:  Business processing class of JDDM sourceDB data engine | Jddm Hive 引擎 IceBerg 业务处理类
 * @author（创建人）: JH 
 * @createDate（创建时间）: datetime  
 * @updateUser（修改人）: JH 
 * @updateDate（修改时间）: 2023/06/29  AM 0:50
 * @updateRemark（修改备注）: Added annotation of this method| 增加该方法的注解。
 * @version（版本）: v1.0.0.0
 */
public class OperationTotalSyncByIceBergThreadPool extends Thread{

//	private Logger log = LogManager.getLogger(this.getClass());
	public Logger log = LogManager.getLogger(OperationTotalSyncByIceBergThreadPool.class);
	
	private Lock threadlock = new ReentrantLock();
	public OperationTotalSyncByIceBergThreadPool() {
		
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		//super.run();
		Object recvPackageObj=null;
		PackageReturnVo packageReturnVo = null;
		
		long startTimer=0L;
		String schemaKeyByParquet="";
		String schemaKeyByParquetThreadID="";
		String jddmEngineWriteFileName="";
		int rowsNum=0;
		Map<String, Udb_BcolumnVo> rowUdbColumnMap = new HashMap<String, Udb_BcolumnVo>();
		int columnsNum=0;
		//Group group = null;
		//ParquetWriter<Group> parQuetwriter = null;
		//GroupFactory factory = null;
		String jddmEngineWriteAllFileInfo="";
		long threadID = Thread.currentThread().getId();
		
		ImmutableList.Builder<GenericRecord> immTableBuilder = null;
		GlobalConfInfo.txtFileNoSpliMap.put(threadID, new AtomicInteger(0));
		String colNameByNumberKey="";
		
		while(true){
			
			try {

				
				if(ConstantPublic.jddmEngineStatFlag){

					recvPackageObj = GlobalSetConfInfo.icebergEngineOperationQueue.take();
					//recvPackageObj = GlobalConfInfoSet.jddm
					if(recvPackageObj instanceof PackageReturnVo) {
						
						packageReturnVo = (PackageReturnVo)recvPackageObj;
						rowUdbColumnMap=packageReturnVo.getRowUdbColumnMap();
						rowsNum= Integer.parseInt(packageReturnVo.getRowsCount());
						columnsNum=Integer.parseInt(packageReturnVo.getColsCount());
						schemaKeyByParquet = packageReturnVo.getOwnerName().toLowerCase()+"."+packageReturnVo.getTableName().toLowerCase();
						schemaKeyByParquetThreadID = packageReturnVo.getOwnerName().toLowerCase()+"."+packageReturnVo.getTableName().toLowerCase()+"."+threadID;
						if(!GlobalConfInfo.engineAtomicByTableKeyMap.containsKey(schemaKeyByParquetThreadID)) {
							GlobalConfInfo.engineAtomicByTableKeyMap.put(schemaKeyByParquetThreadID,new AtomicInteger(1));
						}else {
							GlobalConfInfo.engineAtomicByTableKeyMap.get(schemaKeyByParquetThreadID).getAndIncrement();
						}
						
						//GlobalConfCommInfo.parallelOperCounterMap.get(packageReturnRowVo.getThisThreadID()).getAndDecrement();

						if(!GlobalSetConfInfo.IceBergTableGnericCacheMap.containsKey(schemaKeyByParquetThreadID)) {
						
							startTimer = System.currentTimeMillis();
							
							// 1. 构建记录
							GenericRecord record = null;
							if (GlobalSetConfInfo.IceBergSchemaCahceMap.isEmpty() || GlobalSetConfInfo.IceBergSchemaCahceMap.get(schemaKeyByParquet)==null){
									GlobalConfCommInfo.ddlOperCacheTableKeyMap.put(schemaKeyByParquet,schemaKeyByParquet);
									while (true){
										if (GlobalSetConfInfo.IceBergSchemaCahceMap.get(schemaKeyByParquet)!=null){
											record = GenericRecord.create(GlobalSetConfInfo.IceBergSchemaCahceMap.get(schemaKeyByParquet));
											break;
										}

									}

							}else if (GlobalSetConfInfo.IceBergSchemaCahceMap.get(schemaKeyByParquet)!=null) {
								record = GenericRecord.create(GlobalSetConfInfo.IceBergSchemaCahceMap.get(schemaKeyByParquet));

							}else {
								throw new NullPointerException();
							}

							immTableBuilder = ImmutableList.builder();
						
							GlobalSetConfInfo.IceBergTableGnericCacheMap.put(schemaKeyByParquetThreadID, record);
							GlobalSetConfInfo.IceBergSchemaImmuTableRecordMap.put(schemaKeyByParquetThreadID, immTableBuilder);
//							GlobalSetConfInfo.IceBergSchemaImmuTableDeleteRecordMap.put(schemaKeyByParquetThreadID, immTableBuilder);
							log.info(" IceBreg Module Init Using Timer ("+(System.currentTimeMillis()-startTimer)+") ...");

						}
						
						/*** *** 
						builder.add(ImmutableMap.of("id", 1, "name", "chen", "birth", "2020-03-08"));
						builder.add(ImmutableMap.of("id", 2, "name", "yuan", "birth", "2021-03-09"));
						builder.add(ImmutableMap.of("id", 3, "name", "jie", "birth", "2023-03-10"));
						builder.add(ImmutableMap.of("id", 4, "name", "ma", "birth", "2023-03-11"));
						***/
						
						Udb_BcolumnVo columnInfo = null;
						// 1. 构建记录

						for(int rowNo=0;rowNo < rowsNum;rowNo++) {
							GenericRecord iceBergRecord = GenericRecord.create(GlobalSetConfInfo.IceBergSchemaCahceMap.get(schemaKeyByParquet));
							GenericRecord deleteRecord = GenericRecord.create(GlobalSetConfInfo.IceBergSchemaCahceMap.get(schemaKeyByParquet));
							for (int colNo = 0; colNo < columnsNum; colNo++) {

								columnInfo = (Udb_BcolumnVo) rowUdbColumnMap.get(rowNo + "-" + colNo);
								colNameByNumberKey = schemaKeyByParquet + "." + columnInfo.getColumnName().toLowerCase();
								switch (packageReturnVo.getOperationType().toUpperCase()) {
									case "I":
//									System.out.println("----columnName: "+columnInfo.getColumnName()+" colValue: "+columnInfo.getColumnValue()+" ColType: "+columnInfo.getColTypeArr()[1]);
										if (GlobalConfCommInfo.jddmEngineTypeByYloaderColMap.containsKey(colNameByNumberKey)) {
											if (columnInfo.getColumnValue() != null && !columnInfo.getColumnValue().equals("")) {
												//group.add(columnInfo.getColumnName().toLowerCase(), columnInfo.getColumnValue());
												iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), columnInfo.getColumnValue());
											}
										} else {
//										System.out.println("----columnName: "+columnInfo.getColumnName()+" colValue: "+columnInfo.getColumnValue()+" ColType: "+columnInfo.getColTypeArr()[1]);
											switch (columnInfo.getColTypeArr()[1]) {
												case 0x02:
													if (columnInfo.getColumnValue() == null || columnInfo.getColumnValue().equals("")) {

														iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), null);

													} else {

														if (GlobalConfCommInfo.jddmEngineTypeByNumberColMap.containsKey(colNameByNumberKey)) {
//														log.info("--date_pro[ColumnType.Number]-- >> "+columnInfo.getColumnName() +" Value :: " +columnInfo.getColumnValue() + " Prosicon :: "+GlobalSetConfInfo.jddmEngineTypeByNumberColMap.get(colNameByNumberKey));


															switch (GlobalConfCommInfo.jddmEngineTypeByNumberColMap.get(colNameByNumberKey)) {

																case 1000: //NUMBER
																	iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), columnInfo.getColumnValue());
																	break;
																case 1100: //NUMBER(*, 0)
																	iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), columnInfo.getColumnValue());
																	break;
																case 1200: //NUMBER(%d)
																	iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), columnInfo.getColumnValue());
																	break;
																case 3000: //NUMBER(%d,%d)
																	iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), Double.parseDouble(columnInfo.getColumnValue()));
																	break;
																case 3100: //FLOAT(%d) |DOUBLE(%d)
																	iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), columnInfo.getColumnValue());
																	break;
															}
														/*switch(GlobalConfCommInfo.jddmEngineTypeByNumberColMap.get(colNameByNumberKey)) {

															case 1000: //NUMBER
																iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), new BigDecimal(columnInfo.getColumnValue()).setScale(18,RoundingMode.HALF_UP));
																break;
															case 1100: //NUMBER(*, 0)
																iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), new BigDecimal(columnInfo.getColumnValue()).setScale(0,RoundingMode.HALF_UP));
																break;
															case 1200: //NUMBER(%d)
																iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), new BigDecimal(columnInfo.getColumnValue()).setScale(0,RoundingMode.HALF_UP));
																break;
															case 3000: //NUMBER(%d,%d)
																iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), new BigDecimal(columnInfo.getColumnValue()).setScale(GlobalSetConfInfo.jddmEngineTypeByNumberColMap.get(colNameByNumberKey),RoundingMode.HALF_UP));
																break;
															case 3100: //FLOAT(%d) |DOUBLE(%d)
																iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), new BigDecimal(columnInfo.getColumnValue()).setScale(0,RoundingMode.HALF_UP));
																break;show
														}*/

														}
													}

													break;
											/*case 0x12: //0x0c ---> date (2012-12-12 12:12:12)
												if (columnInfo.getColumnValue()==null||columnInfo.getColumnValue().equals("")){
													iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), null);
													break;
												}else {
													DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
													iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(),LocalDateTime.parse(columnInfo.getColumnValue(), formatter));
													break;
												}*/
												case -75: //0xb5 --->TIMESTAMP 2012-12-12 12:12:12.123456789 +时区
												case -76: //0xb4 --->TIMESTAMP 2012-12-12 12:12:12.123456789
													if (columnInfo.getColumnValue() == null || columnInfo.getColumnValue().equals("")) {
														iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), null);
														break;
													} else {
														DateTimeFormatter timestampWithZoneFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
														iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), LocalDateTime.parse(columnInfo.getColumnValue(), timestampWithZoneFormatter));
														break;
													}
												case 0x64:  //int:100 ; Binary_Float
												case 0x65:  //int:101 ; Binary_Double
													if (columnInfo.getColumnValue() != null && !columnInfo.getColumnValue().equals("")) {
														//group.add(columnInfo.getColumnName().toLowerCase(), Double.parseDouble(columnInfo.getColumnValue()));
														iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), Double.parseDouble(columnInfo.getColumnValue()));
													}
													//log.info(" group.add("+columnInfo.getColumnName().toLowerCase()+","+columnInfo.getColumnValue()+") double --> ::"+ConversionUtil.bytesToHexString(columnInfo.getColTypeArr()));
													break;
												case 0x71:

													if (GlobalConfCommInfo.jddmEngineTypeBy0x71BytesColMap.containsKey(colNameByNumberKey)) {
														if (columnInfo.getColumnValue() != null && !columnInfo.getColumnValue().equals("")) {


															//log.info(columnInfo.getColumnName()+" ---> "+columnInfo.getColumnValue()+"  "+ConversionUtil.hexStringsToBytes(columnInfo.getColumnValue()).length);

															//ByteBuffer setbyteBuffer = ByteBuffer.allocate(ConversionUtil.hexStringsToBytes(columnInfo.getColumnValue()).length);
															//setbyteBuffer.put(ConversionUtil.hexStringsToBytes(columnInfo.getColumnValue()));
															//setbyteBuffer.flip();
															//iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), setbyteBuffer);
															iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), columnInfo.getColumnValue());
														}
													} else {
														if (columnInfo.getColumnValue() != null && !columnInfo.getColumnValue().equals("")) {
															//group.add(columnInfo.getColumnName().toLowerCase(), Double.parseDouble(columnInfo.getColumnValue()));
															iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), new String(ConversionUtil.hexStringsToBytes(columnInfo.getColumnValue())));
														}
													}

													break;
												default:
													if (columnInfo.getColumnValue() != null && !columnInfo.getColumnValue().equals("")) {
														//group.add(columnInfo.getColumnName().toLowerCase(), columnInfo.getColumnValue());
														iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), columnInfo.getColumnValue());

//											log.info(packageReturnRowVo.getOperationType()+" ---> "+columnInfo.getCflag()+" record.add("+columnInfo.getColumnName().toLowerCase()+",[Bytes[] :"+ ConversionUtil.bytesToHexString(columnInfo.getColumnValue().getBytes()) +"]) TypeArr --> ::"+ConversionUtil.bytesToHexString(columnInfo.getColTypeArr())+" "+GlobalConfCommInfo.jddmEngineTypeByNumberColMap.get(schemaKeyByParquet+"."+columnInfo.getColumnName().toLowerCase()));


													}
													//log.info(" group.add("+columnInfo.getColumnName().toLowerCase()+","+columnInfo.getColumnValue()+") binary --> ::"+ConversionUtil.bytesToHexString(columnInfo.getColTypeArr()));
													break;
											}
										}
										break;
/*								case "D":
//									Parquet.WriteBuilder writeBuilder = GlobalSetConfInfo.IceBergCacheTableMap.get(schemaKeyByParquet);
									*//*Snapshot newSnapshotId = GlobalSetConfInfo.IceBergCacheTableMap.get(schemaKeyByParquet)
											.newScan()
											.filter(Expressions.notEqual("column_name", "value_to_delete"))
											.snapshot();*//*
									System.out.println("################DELETE");
									if(GlobalConfInfo.engineAtomicByTableKeyMap.get(schemaKeyByParquetThreadID).get() % Constant.writeCountNoToHiveFile == 0) {

										Expression deleteCondition = Expressions.equal("id", "1");
									*//*DeleteFiles deleteFiles = GlobalSetConfInfo.IceBergCacheTableMap.get(schemaKeyByParquet).newDelete();
									deleteFiles.deleteFromRowFilter(deleteCondition);
									deleteFiles.commit();*//*

										System.out.println("############Table:: " + GlobalSetConfInfo.IceBergCacheTableMap.get(schemaKeyByParquet));
										Transaction t = GlobalSetConfInfo.IceBergCacheTableMap.get(schemaKeyByParquet).newTransaction();
										t.newDelete().deleteFromRowFilter(deleteCondition).commit();

									}
									break;*/
									case "D":
										if ((columnInfo.getCflag() & 8) > 0) {
											System.out.println("---------->>>Del_Col: " + columnInfo.getColumnName() + " ->Val: " + columnInfo.getColumnValue() + " cflag: " + columnInfo.getCflag());
											deleteRecord.setField(columnInfo.getColumnName().toLowerCase(), columnInfo.getColumnValue());
										}
										break;

									default:
										if (GlobalConfCommInfo.jddmEngineTypeByYloaderColMap.containsKey(colNameByNumberKey)) {
											if (columnInfo.getColumnValue() != null && !columnInfo.getColumnValue().equals("")) {
												//group.add(columnInfo.getColumnName().toLowerCase(), columnInfo.getColumnValue());
												iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), columnInfo.getColumnValue());
											}
										} else {
//										System.out.println("----columnName: "+columnInfo.getColumnName()+" colValue: "+columnInfo.getColumnValue()+" ColType: "+columnInfo.getColTypeArr()[1]);
											switch (columnInfo.getColTypeArr()[1]) {
												case (byte) 0x189:
													break;
												case 0x02:
													if (columnInfo.getColumnValue() == null || columnInfo.getColumnValue().equals("")) {

														iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), null);

													} else {

														if (GlobalConfCommInfo.jddmEngineTypeByNumberColMap.containsKey(colNameByNumberKey)) {
															switch (GlobalConfCommInfo.jddmEngineTypeByNumberColMap.get(colNameByNumberKey)) {

																case 1000: //NUMBER
																	iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), columnInfo.getColumnValue());
																	break;
																case 1100: //NUMBER(*, 0)
																	iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), columnInfo.getColumnValue());
																	break;
																case 1200: //NUMBER(%d)
																	iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), columnInfo.getColumnValue());
																	break;
																case 3000: //NUMBER(%d,%d)
																	iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), Double.parseDouble(columnInfo.getColumnValue()));
																	break;
																case 3100: //FLOAT(%d) |DOUBLE(%d)
																	iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), columnInfo.getColumnValue());
																	break;
															}
/*														switch(GlobalConfCommInfo.jddmEngineTypeByNumberColMap.get(colNameByNumberKey)) {

															case 1000: //NUMBER
																iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), new BigDecimal(columnInfo.getColumnValue()).setScale(18,RoundingMode.HALF_UP));
																break;
															case 1100: //NUMBER(*, 0)
																iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), new BigDecimal(columnInfo.getColumnValue()).setScale(0,RoundingMode.HALF_UP));
																break;
															case 1200: //NUMBER(%d)
																iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), new BigDecimal(columnInfo.getColumnValue()).setScale(0,RoundingMode.HALF_UP));
																break;
															case 3000: //NUMBER(%d,%d)
																iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), new BigDecimal(columnInfo.getColumnValue()).setScale(GlobalSetConfInfo.jddmEngineTypeByNumberColMap.get(colNameByNumberKey),RoundingMode.HALF_UP));
																break;
															case 3100: //FLOAT(%d) |DOUBLE(%d)
																iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), new BigDecimal(columnInfo.getColumnValue()).setScale(0,RoundingMode.HALF_UP));
																break;
														}*/

														}
													}

													break;
												case -75: //0xb5 --->TIMESTAMP 2012-12-12 12:12:12.123456789 +时区
												case -76: //0xb4 --->TIMESTAMP 2012-12-12 12:12:12.123456789
													if (columnInfo.getColumnValue() == null || columnInfo.getColumnValue().equals("")) {
														iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), null);
														break;
													} else {
														DateTimeFormatter timestampWithZoneFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
														iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), LocalDateTime.parse(columnInfo.getColumnValue(), timestampWithZoneFormatter));
														break;
													}
                                            /*case 0x12: //0x0c ---> date (2012-12-12 12:12:12)
												if (columnInfo.getColumnValue()==null||columnInfo.getColumnValue().equals("")){
													iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), null);
													break;
												}else {
													DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
													iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(),LocalDateTime.parse(columnInfo.getColumnValue(), formatter));
													break;
												}*/

												case 0x64:  //int:100 ; Binary_Float
												case 0x65:  //int:101 ; Binary_Double
													if (columnInfo.getColumnValue() != null && !columnInfo.getColumnValue().equals("")) {
														//group.add(columnInfo.getColumnName().toLowerCase(), Double.parseDouble(columnInfo.getColumnValue()));
														iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), Double.parseDouble(columnInfo.getColumnValue()));
													}
													//log.info(" group.add("+columnInfo.getColumnName().toLowerCase()+","+columnInfo.getColumnValue()+") double --> ::"+ConversionUtil.bytesToHexString(columnInfo.getColTypeArr()));
													break;
												case 0x70:
												case 0x71:

													if (GlobalConfCommInfo.jddmEngineTypeBy0x71BytesColMap.containsKey(colNameByNumberKey)) {
														if (columnInfo.getColumnValue() != null && !columnInfo.getColumnValue().equals("")) {


															//log.info(columnInfo.getColumnName()+" ---> "+columnInfo.getColumnValue()+"  "+ConversionUtil.hexStringsToBytes(columnInfo.getColumnValue()).length);

															//ByteBuffer setbyteBuffer = ByteBuffer.allocate(ConversionUtil.hexStringsToBytes(columnInfo.getColumnValue()).length);
															//setbyteBuffer.put(ConversionUtil.hexStringsToBytes(columnInfo.getColumnValue()));
															//setbyteBuffer.flip();
															//iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), setbyteBuffer);
															iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), columnInfo.getColumnValue());
														}
													} else {
														if (columnInfo.getColumnValue() != null && !columnInfo.getColumnValue().equals("")) {
															//group.add(columnInfo.getColumnName().toLowerCase(), Double.parseDouble(columnInfo.getColumnValue()));
															iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), new String(ConversionUtil.hexStringsToBytes(columnInfo.getColumnValue())));
														}
													}
													break;

												default:
													if (columnInfo.getColumnValue() != null && !columnInfo.getColumnValue().equals("")) {
														//group.add(columnInfo.getColumnName().toLowerCase(), columnInfo.getColumnValue());
														iceBergRecord.setField(columnInfo.getColumnName().toLowerCase(), columnInfo.getColumnValue());

//											log.info(packageReturnRowVo.getOperationType()+" ---> "+columnInfo.getCflag()+" record.add("+columnInfo.getColumnName().toLowerCase()+",[Bytes[] :"+ ConversionUtil.bytesToHexString(columnInfo.getColumnValue().getBytes()) +"]) TypeArr --> ::"+ConversionUtil.bytesToHexString(columnInfo.getColTypeArr())+" "+GlobalConfCommInfo.jddmEngineTypeByNumberColMap.get(schemaKeyByParquet+"."+columnInfo.getColumnName().toLowerCase()));


													}
													//log.info(" group.add("+columnInfo.getColumnName().toLowerCase()+","+columnInfo.getColumnValue()+") binary --> ::"+ConversionUtil.bytesToHexString(columnInfo.getColTypeArr()));
													break;
											}
										}
										break;

								}

								if (ConstantPubSet.logForAgentType == 2000) {


//								log.info(packageReturnRowVo.getOperationType()+" -> "+columnInfo.getCflag()+" record.add("+columnInfo.getColumnName().toLowerCase()+",["+columnInfo.getColumnValue()+"]) TypeArr --> ::"+ConversionUtil.bytesToHexString(columnInfo.getColTypeArr())+" "+GlobalConfCommInfo.jddmEngineTypeByNumberColMap.get(schemaKeyByParquet+"."+columnInfo.getColumnName().toLowerCase()));
								}


							}

							//log.info(" ");

							if (GlobalSetConfInfo.IceBergSchemaImmuTableRecordMap.get(schemaKeyByParquetThreadID) != null) {

								GlobalSetConfInfo.IceBergSchemaImmuTableRecordMap.get(schemaKeyByParquetThreadID).add(iceBergRecord);
							} else {
//								log.info("############原子计数器:"+GlobalConfInfo.engineAtomicByTableKeyMap.get(schemaKeyByParquetThreadID)+" 统计数量:"+Constant.writeCountNoToHiveFile);
								throw new NullPointerException();

							}
/*						if (GlobalSetConfInfo.IceBergSchemaImmuTableDeleteRecordMap.get(schemaKeyByParquetThreadID)!=null){

							GlobalSetConfInfo.IceBergSchemaImmuTableDeleteRecordMap.get(schemaKeyByParquetThreadID).add(deleteRecord);
						}else{
//								log.info("############原子计数器:"+GlobalConfInfo.engineAtomicByTableKeyMap.get(schemaKeyByParquetThreadID)+" 统计数量:"+Constant.writeCountNoToHiveFile);
							throw new NullPointerException();

						}*/
							iceBergRecord = null;
						}
						while (true){
							if (GlobalSetConfInfo.IceBergCacheTableMap.containsKey(schemaKeyByParquet)){
								break;
							}
							try {
								Thread.sleep(1000); // 休眠1秒，防止占用过高cpu
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						}


						GlobalConfInfo.lastDataWriteTimerByParquetMap.put(schemaKeyByParquetThreadID, System.currentTimeMillis());

						if(GlobalConfInfo.engineAtomicByTableKeyMap.get(schemaKeyByParquetThreadID).get() % Constant.writeCountNoToHiveFile == 0) {
							GlobalConfInfo.lastDataWriteTimerByParquetMap.put(schemaKeyByParquetThreadID, System.currentTimeMillis());
								log.info(" threadID ::"+Thread.currentThread().getId()+"  --->"+packageReturnVo.getOwnerName()+" tableName ::"+packageReturnVo.getTableName()
									+" columnSize ::"+packageReturnVo.getRowsCount()+" nowCount ::"+GlobalConfInfo.engineAtomicByTableKeyMap.get(schemaKeyByParquetThreadID).get()+" File::"+GlobalSetConfInfo.IceBergTableCacheFileMap.get(schemaKeyByParquetThreadID));
							
								DataFileToIceBergOperation dataFileToIceBergOperation = new DataFileToIceBergOperation();
								
								dataFileToIceBergOperation.setSchemaKeyByParquet(schemaKeyByParquet);
								dataFileToIceBergOperation.setSchemaKeyByParquetThreadID(schemaKeyByParquetThreadID);
								dataFileToIceBergOperation.run();
								
								
								while(true) {
									
									for(Map.Entry<String, Boolean> completeMap:GlobalSetConfInfo.IceBergOperationCompleteMap.entrySet()) {
										
										log.info("  --------> "+completeMap.getKey()+" value ::"+completeMap.getValue()+" ["+GlobalSetConfInfo.IceBergOperationBeginMap.size()+"="+GlobalSetConfInfo.IceBergOperationCompleteMap.size()+"]");
									}
									
									
									if(GlobalSetConfInfo.IceBergOperationBeginMap.size()==GlobalSetConfInfo.IceBergOperationCompleteMap.size()) {
										
										Constant.writeToIceBergDBFlag = true;
										GlobalSetConfInfo.IceBergOperationCompleteMap.clear();
										GlobalSetConfInfo.IceBergOperationBeginMap.clear();
										break;
									}else {
										Thread.sleep(1000);
									}
								}
								
								log.info(" JddmForIceBerg Batch Operation Complete ... ... ");
								
							  /***
								GlobalSetConfInfo.IceBergSchemaImmuTableRecordMap.get(schemaKeyByParquetThreadID).build();
								
								//String filepath = GlobalSetConfInfo.IceBergCacheTableMap.get(schemaKeyByParquet).location() + "/" + UUID.randomUUID().toString();
								
								GlobalSetConfInfo.IceBergTableCacheFileMap.putIfAbsent(schemaKeyByParquetThreadID, GlobalSetConfInfo.IceBergCacheTableMap.get(schemaKeyByParquet).location() + "/" + UUID.randomUUID().toString());
								
		OutputFile file = GlobalSetConfInfo.IceBergCacheTableMap.get(schemaKeyByParquet).io().newOutputFile(GlobalSetConfInfo.IceBergTableCacheFileMap.get(schemaKeyByParquetThreadID));
								DataWriter<GenericRecord> dataWriter =
								    Parquet.writeData(file)
								    .schema(GlobalSetConfInfo.IceBergSchemaCahceMap.get(schemaKeyByParquet))
								    .createWriterFunc(GenericParquetWriter::buildWriter)
								    .overwrite()
								    .withSpec(PartitionSpec.unpartitioned())
								    .build();
	
								DataFile dataFile = null;
								try {
								    for (GenericRecord record : GlobalSetConfInfo.IceBergSchemaImmuTableRecordMap.get(schemaKeyByParquetThreadID).build()) {
								        dataWriter.write(record);
								    }
								    
								} finally {
								    dataWriter.close();
								}
								
								 // 3. 将文件写入table中
								dataFile = dataWriter.toDataFile();
								GlobalSetConfInfo.IceBergCacheTableMap.get(schemaKeyByParquet).newAppend().appendFile(dataFile).commit();
								
								
								GlobalSetConfInfo.IceBergTableGnericCacheMap.remove(schemaKeyByParquetThreadID);
	
								immTableBuilder = null;
							    dataFile = null;
							    
							    ***/
							
						}
					}
				}


			}catch(Exception ex) {
				ex.printStackTrace();
				log.info(" Exception threadID ::"+Thread.currentThread().getId());
				break;
			}
			
		}
	}


}
