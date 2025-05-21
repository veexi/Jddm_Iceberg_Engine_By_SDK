package com.jddm.thread;

import com.jddm.common.Constant;
import com.jddm.conf.GlobalConfInfo;
import com.jddm.conf.GlobalSetConfInfo;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
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
//							.addRows(dataWriter.toDataFile())
							.commit();

//					GlobalSetConfInfo.IceBergSchemaImmuTableDeleteRecordMap.remove(immuTableKeyName); // 删除缓存
				} else {
					// 只追加数据文件（你原来的逻辑）
					GlobalSetConfInfo.IceBergCacheTableMap.get(tableKeyName)
							.newAppend().appendFile(dataWriter.toDataFile()).commit();
				}


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
