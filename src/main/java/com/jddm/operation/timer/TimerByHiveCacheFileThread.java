package com.jddm.operation.timer;

import com.jddm.common.Constant;
import com.jddm.conf.GlobalConfInfo;
import com.jddm.conf.GlobalSetConfInfo;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;


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
		Set<Object> newIds = null;
		String immuTableKeyName="";
		DataFile dataFile = null;
		try{
//			System.out.println("----->>>>GlobalConfInfo.lastDataWriteTimerByParquetMap.size:: "+GlobalConfInfo.lastDataWriteTimerByParquetMap.size());
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
					    .overwrite(true)
					    .withSpec(PartitionSpec.unpartitioned())
					    .build();



// 核心改造：DataFileToIceBergOperation.java (约 81 行附近)
                        List<GenericRecord> deleteRecords = GlobalSetConfInfo.IceBergSchemaImmuTableDeleteRecordMap.get(immuTableKeyName).build();

                        if (deleteRecords != null && !deleteRecords.isEmpty()) {
                            Table iceBergTable = GlobalSetConfInfo.IceBergCacheTableMap.get(tableKeyName);

                            // 1. 动态获取主键列表
                            List<String> pkNames = GlobalSetConfInfo.TablePkColCacheMap.get(tableKeyName);
                            List<Integer> equalityFieldIds;

                            if (pkNames != null && !pkNames.isEmpty()) {
                                // 【情况 A】有主键表：仅提取主键列的 fieldId
                                equalityFieldIds = pkNames.stream()
                                        .map(name -> iceBergTable.schema().findField(name).fieldId())
                                        .collect(Collectors.toList());
                            } else {
                                // 【情况 B】无主键表（生产兜底方案）：将该表的所有列作为 Equality 识别条件 (全字段精确删除)
                                equalityFieldIds = iceBergTable.schema().columns().stream()
                                        .map(org.apache.iceberg.types.Types.NestedField::fieldId)
                                        .collect(Collectors.toList());
                            }

                            OutputFile deleteOut = iceBergTable.io().newOutputFile(
                                    iceBergTable.location() + "/delete/eq_del_" + UUID.randomUUID());

                            // 2. 构建通用的 EqualityDeleteWriter
                            EqualityDeleteWriter<GenericRecord> deleteWriter = Parquet.writeDeletes(deleteOut)
                                    .forTable(iceBergTable)
                                    .createWriterFunc(GenericParquetWriter::buildWriter)
                                    .equalityFieldIds(equalityFieldIds) // 传入动态算出的标识列 IDs
                                    .buildEqualityWriter();

                            // 3. 写入删除记录 (对于无主键表，deleteRecord 里必须包含所有列的完整旧值)
                            for (GenericRecord rec : deleteRecords) {
                                deleteWriter.write(rec);
                            }
                            deleteWriter.close();

                            // 4. 与数据插入文件一并 Commit
                            iceBergTable.newRowDelta()
                                    .addDeletes(deleteWriter.result().deleteFiles().get(0))
                                    .addRows(dataWriter.toDataFile()) // 兼容有新增数据的情况
                                    .commit();
                        }
					dataFile = dataWriter.toDataFile();
					GlobalSetConfInfo.IceBergCacheTableMap.get(tableKeyName).newOverwrite().addFile(dataFile).overwriteByRowFilter(Expressions.and(Expressions.equal("id","1"),Expressions.equal("name","2"))).commit();
					log.info(" TimerBatch =====>>> "+"ThreadID Key ::"+cacheTimerMap.getKey()+" File ::"+filepath+" Count::"+GlobalSetConfInfo.IceBergSchemaImmuTableRecordMap.get(immuTableKeyName).build().size()+" ... ");
//					log.info(" TimerBatch =====>>> "+"ThreadID Key ::"+cacheTimerMap.getKey()+" File ::"+filepath+" Count:: [I]="+GlobalSetConfInfo.IceBergSchemaImmuTableRecordMap.get(immuTableKeyName).build().size()+" [D]="+GlobalSetConfInfo.IceBergSchemaImmuTableDeleteRecordMap.get(immuTableKeyName).build().size()+" CompletSize ::["+GlobalSetConfInfo.IceBergOperationCompleteMap.size()+"] ... ");
					GlobalSetConfInfo.IceBergTableGnericCacheMap.remove(immuTableKeyName);
					GlobalSetConfInfo.IceBergSchemaImmuTableRecordMap.remove(immuTableKeyName);
//					GlobalSetConfInfo.IceBergSchemaImmuTableDeleteRecordMap.remove(immuTableKeyName);
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
