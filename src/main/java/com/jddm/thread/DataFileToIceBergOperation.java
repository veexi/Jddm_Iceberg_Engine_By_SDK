package com.jddm.thread;

import com.jddm.common.Constant;
import com.jddm.conf.GlobalConfInfo;
import com.jddm.conf.GlobalSetConfInfo;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class DataFileToIceBergOperation extends Thread{

//	private static Logger log = LogManager.getLogger(DataFileToIceBergOperation.class);
public Logger log = LogManager.getLogger(DataFileToIceBergOperation.class);
	
	private String schemaKeyByParquetThreadID;
	private String schemaKeyByParquet;

	Lock lock = new ReentrantLock();

	@Override
	public void run() {
		// TODO Auto-generated method stub
		//super.run();
		
		String[] splitArr = null;
		String immuTableKeyName="";
		String tableKeyName="";

		
		
		//synchronized(this) {
		if(Constant.writeToIceBergDBFlag) {	
			Constant.writeToIceBergDBFlag = false;
			DataFile dataFile = null;
			try {
					GlobalSetConfInfo.IceBergOperationBeginMap.putIfAbsent(schemaKeyByParquetThreadID, new AtomicInteger(0));
					//log.info(" =============================================================================== "+Thread.currentThread().getId());
					splitArr = schemaKeyByParquetThreadID.split("[.]");
					immuTableKeyName = schemaKeyByParquetThreadID;
					
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
					    for (GenericRecord record : GlobalSetConfInfo.IceBergSchemaImmuTableRecordMap.get(schemaKeyByParquetThreadID).build()) {
					        dataWriter.write(record);
					    }
					    
					} finally {
					    dataWriter.close();
					    
					}
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
/*				List<GenericRecord> deleteRecords = GlobalSetConfInfo.IceBergSchemaImmuTableDeleteRecordMap.get(immuTableKeyName).build();

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

					GlobalSetConfInfo.IceBergSchemaImmuTableDeleteRecordMap.remove(immuTableKeyName); // 删除缓存
				} else {
					// 只追加数据文件（你原来的逻辑）
					GlobalSetConfInfo.IceBergCacheTableMap.get(tableKeyName)
							.newAppend()
							.appendFile(dataWriter.toDataFile())
							.commit();
				}*/


				// 3. 将文件写入table中

/*				dataFile = dataWriter.toDataFile();
				GlobalSetConfInfo.IceBergCacheTableMap.get(tableKeyName).newAppend().appendFile(dataFile).commit();*/

				GlobalSetConfInfo.IceBergTableGnericCacheMap.remove(immuTableKeyName);

				GlobalConfInfo.lastDataWriteTimerByParquetMap.put(schemaKeyByParquetThreadID,System.currentTimeMillis());
				GlobalSetConfInfo.IceBergOperationCompleteMap.put(schemaKeyByParquetThreadID, true);
				log.info(" ThreadBatch =====>>> "+"ThreadID Key ::"+schemaKeyByParquetThreadID+" File ::"+filepath+" Count:: [I]="+GlobalSetConfInfo.IceBergSchemaImmuTableRecordMap.get(immuTableKeyName).build().size()+" [D]="+GlobalSetConfInfo.IceBergSchemaImmuTableDeleteRecordMap.get(immuTableKeyName).build().size()+" CompletSize ::["+GlobalSetConfInfo.IceBergOperationCompleteMap.size()+"] ... ");
				GlobalSetConfInfo.IceBergSchemaImmuTableRecordMap.remove(immuTableKeyName);
				GlobalSetConfInfo.IceBergSchemaImmuTableDeleteRecordMap.remove(immuTableKeyName);

		   
			}catch(Exception ex) {
				ex.printStackTrace();
			}finally {
				 dataFile = null;
			}
			
		}else {
			log.info(" =============================================================================== "+Thread.currentThread().getId()+" Is running ...");
		}
		
	}
	public String getSchemaKeyByParquetThreadID() {
		return schemaKeyByParquetThreadID;
	}
	public void setSchemaKeyByParquetThreadID(String schemaKeyByParquetThreadID) {
		this.schemaKeyByParquetThreadID = schemaKeyByParquetThreadID;
	}
	public String getSchemaKeyByParquet() {
		return schemaKeyByParquet;
	}
	public void setSchemaKeyByParquet(String schemaKeyByParquet) {
		this.schemaKeyByParquet = schemaKeyByParquet;
	}

	
	 
	
}
