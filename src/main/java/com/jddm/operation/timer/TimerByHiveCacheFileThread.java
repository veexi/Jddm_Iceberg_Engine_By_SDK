package com.jddm.operation.timer;

import com.jddm.common.Constant;
import com.jddm.conf.GlobalConfInfo;
import com.jddm.conf.GlobalSetConfInfo;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;


public class TimerByHiveCacheFileThread implements Runnable{
	private static final Logger log = LogManager.getLogger(TimerByHiveCacheFileThread.class);
	
	public TimerByHiveCacheFileThread(){
		
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		getIceBergHiveCacheFiles();
		}
		//tablePartitionOfHiveDataBaseAndDataFileSupplement();

		//System.out.format("%s TIMER [%s] \tCalc-Timer ::%-29s \n",OperationTimes.printDataTime(),"TimerByHiveCacheFileThread","(DSG)HIVE-JDDM-ENGINE Timer Running ...");
	

	/*** *** *** *** 
	* @functionName（方法名称）: operationBy_Part_OrcMulti_HiveCacheFiles 
	* @description （方法说明）: Create Hive multi-partition table in Orc store mode  | 创建Orc存储模式的Hive双分区表；
	* @param       （传入参数1）: NULL ;
	* @return      （返回）   :  void
	* @exception   （异常）   : (throws Exception)
	* @author      （创建人）: JH
	* @since       （创建时间）:  2023/08/13 PM 4:38;
	***/

	
	/*** *** *** *** 
	* @functionName（方法名称）: operationBy_OrcMulti_HiveCacheFiles 
	* @description （方法说明）: Create Hive multi-partition table in Orc store mode  | 创建Orc存储模式的Hive双分区表；
	* @param       （传入参数1）: NULL ;
	* @return      （返回）   :  void
	* @exception   （异常）   : (throws Exception)
	* @author      （创建人）: JH
	* @since       （创建时间）:  2023/05/17 PM 4:32;
	***/

	
	/*** *** *** ***
	* @functionName（方法名称）: getIceBergHiveCacheFiles 
	* @description （方法说明）:Timer to write unchanged data for a long time to the Iceberg table in the Hive DB | 定时器，将长时间无变化的数据写入Hive库的Iceberg表中；
	* @param       （传入参数）: NULL
	* @return      （返回）   :  void
	* @exception   （异常）   : Exception ex
	* @author      （创建人）: JH
	* @since       （创建时间）:  2023/06/29 AM 0:52
	* @ModifyTime  （修改时间）:  
	***/
	public static void getIceBergHiveCacheFiles() {
		String parquetFileName=null;
		String hivePartitionKeyValue=null;
		String[] splitArr=null;
		String tableKeyName="";
		
		String immuTableKeyName="";
		DataFile dataFile = null;
		try{
			for(Entry<String, Long> cacheTimerMap:GlobalConfInfo.lastDataWriteTimerByParquetMap.entrySet()) {
				immuTableKeyName = cacheTimerMap.getKey();
				if((System.currentTimeMillis()-cacheTimerMap.getValue())/1000 > Constant.hiveDiffTimers) {
					if (GlobalSetConfInfo.IceBergSchemaImmuTableRecordMap.containsKey(immuTableKeyName)){
					
					log.info(Constant.JddmEngineTypeInfo+" S&T ::"+cacheTimerMap.getKey()+" IceBreg Write  -> LastModifyTimer :: "+transferLongToDate("yyyy-MM-dd HH:mm:ss",cacheTimerMap.getValue())+" > "+Constant.hiveDiffTimers+" ... ... ");
					
					splitArr = cacheTimerMap.getKey().split("[.]");

					
					tableKeyName = splitArr[0]+"."+splitArr[1];
					GlobalSetConfInfo.IceBergSchemaImmuTableRecordMap.get(immuTableKeyName).build();
					
					String filepath = GlobalSetConfInfo.IceBergCacheTableMap.get(tableKeyName).location() + "/" + UUID.randomUUID().toString();
					
					OutputFile file = GlobalSetConfInfo.IceBergCacheTableMap.get(tableKeyName).io().newOutputFile(filepath);
					DataWriter<GenericRecord> dataWriter =
					    Parquet.writeData(file)
					    .schema(GlobalSetConfInfo.IceBergSchemaCahceMap.get(tableKeyName))
					    .createWriterFunc(GenericParquetWriter::buildWriter)
					    .overwrite()
					    .withSpec(PartitionSpec.unpartitioned())
					    .build();

					
					try {
						//log.info("----------------> Timer WriteSize ::"+GlobalSetConfInfo.IceBergSchemaImmuTableRecordMap.get(cacheTimerMap.getKey()).build().size());
					    for (GenericRecord record : GlobalSetConfInfo.IceBergSchemaImmuTableRecordMap.get(cacheTimerMap.getKey()).build()) {
					    	
					        dataWriter.write(record);
					    }
					    
					} finally {
					    dataWriter.close();
					    
					}
						/*List<GenericRecord> deleteRecords = GlobalSetConfInfo.IceBergSchemaImmuTableDeleteRecordMap.get(immuTableKeyName).build();

						if (deleteRecords != null && !deleteRecords.isEmpty()) {
							// 构建等值删除文件
							List<String> pkNames = Arrays.asList("id","name");
							String finalTableKeyName = tableKeyName;
							List<Integer> pkFieldIds = pkNames.stream()
									.map(name -> GlobalSetConfInfo.IceBergCacheTableMap.get(finalTableKeyName).schema().findField(name).fieldId())
									.collect(Collectors.toList());

							OutputFile deleteOut = GlobalSetConfInfo.IceBergCacheTableMap.get(tableKeyName)
									.io().newOutputFile(GlobalSetConfInfo.IceBergCacheTableMap.get(tableKeyName).location() + "/delete/pk_" + UUID.randomUUID());

							EqualityDeleteWriter<GenericRecord> deleteWriter = Parquet.writeDeletes(deleteOut)
									.forTable(GlobalSetConfInfo.IceBergCacheTableMap.get(tableKeyName))
									.createWriterFunc(GenericParquetWriter::buildWriter)
									.equalityFieldIds(pkFieldIds)
									.buildEqualityWriter();

							for (GenericRecord rec : deleteRecords) {
								deleteWriter.write(rec);
							}
							deleteWriter.close();

							DeleteFile deleteFile = deleteWriter.result().deleteFiles().get(0);

							// 写数据 + 删除一起提交
							GlobalSetConfInfo.IceBergCacheTableMap.get(tableKeyName)
									.newRowDelta()
									.addDeletes(deleteFile)
									.addRows(dataWriter.toDataFile())
									.commit();

//					GlobalSetConfInfo.IceBergSchemaImmuTableDeleteRecordMap.remove(immuTableKeyName); // 删除缓存
						} else {
							// 只追加数据文件（你原来的逻辑）
							GlobalSetConfInfo.IceBergCacheTableMap.get(tableKeyName)
									.newAppend().appendFile(dataWriter.toDataFile()).commit();
						}*/
					// 3. 将文件写入table中
					dataFile = dataWriter.toDataFile();
					GlobalSetConfInfo.IceBergCacheTableMap.get(tableKeyName).newAppend().appendFile(dataFile).commit();
/*
						log.info(" TimerBatch =====>>> "+"ThreadID Key ::"+cacheTimerMap.getKey()+" File ::"+filepath+" Count::"+GlobalSetConfInfo.IceBergSchemaImmuTableRecordMap.get(immuTableKeyName).build().size()+" ... ");
*/
						log.info(" TimerBatch =====>>> "+"ThreadID Key ::"+cacheTimerMap.getKey()+" File ::"+filepath+" Count:: [I]="+GlobalSetConfInfo.IceBergSchemaImmuTableRecordMap.get(immuTableKeyName).build().size()+" [D]="+GlobalSetConfInfo.IceBergSchemaImmuTableDeleteRecordMap.get(immuTableKeyName).build().size()+" CompletSize ::["+GlobalSetConfInfo.IceBergOperationCompleteMap.size()+"] ... ");

						GlobalSetConfInfo.IceBergTableGnericCacheMap.remove(immuTableKeyName);
					GlobalSetConfInfo.IceBergSchemaImmuTableRecordMap.remove(immuTableKeyName);
						GlobalSetConfInfo.IceBergSchemaImmuTableDeleteRecordMap.remove(immuTableKeyName);
						GlobalConfInfo.lastDataWriteTimerByParquetMap.remove(cacheTimerMap.getKey());
					
				}
				}
			}
		}catch(Exception ex) {
			ex.printStackTrace();
		}finally {
			dataFile = null;
		}
	}
	

	

	
	
	
	
	/*****
	 * Add TDH（Transwarp）大数据集群，HoloDesk存储方式表的处理；
	 * 2022/12/27 5:23 PM;
	 * Auth:JH
	 * 
	 */




	

	/**
     * 把long 转换成 日期 再转换成String类型
     */
    public static String transferLongToDate(String dateFormat, Long millSec) {
        SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
        Date date = new Date(millSec);
        return sdf.format(date);
    }
	
}
