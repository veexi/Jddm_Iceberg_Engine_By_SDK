package com.jddm.killOper;

import com.jddm.common.Constant;
import com.jddm.conf.GlobalConfInfo;
import com.jddm.conf.GlobalSetConfInfo;
import com.publics.common.ConstantPublic;
import com.publics.conf.GlobalConfCommInfo;
import com.publics.engine.state.EngineStateInfo;
import com.publics.utils.OperationTimes;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.log4j.Logger;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class JddmEngineKillHandler implements SignalHandler{
	private Logger log = Logger.getLogger(this.getClass());
	
	private final long throttleInterval;
	private final TimeUnit timeUnit;
	private final AtomicBoolean isAllowedToRun = new AtomicBoolean(true);
	
	public JddmEngineKillHandler(long throttleInterval, TimeUnit timeUnit) {
		this.throttleInterval = throttleInterval;
        this.timeUnit = timeUnit;
	}
	
	public void registerSignal(String signalName) {
		
		Signal exitSignal = new Signal(signalName);
		//Signal.handle(signal, this);
		Signal.handle(exitSignal, (signal) -> {
            if (isAllowedToRun.compareAndSet(true, false)) {
            	this.handle(signal);
                try {
					timeUnit.sleep(this.throttleInterval);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
                isAllowedToRun.set(true);
            }
        });
		
	}

	@Override
	public void handle(Signal signal) {
		// TODO Auto-generated method stub

		if (ConstantPublic.jddmEngineStatFlag && GlobalConfCommInfo.jddmEngineFullSyncOperationQueue.size() == 0) {
			ConstantPublic.jddmEngineError_Msg = "stop Jddm Engine";
			ConstantPublic.jddmEngineStatFlag = false;
			Constant.customJddmEngineErrorFlag = 10004;
			System.out.println(" ");


			switch (signal.getNumber()) {
				case 2:
					System.out.println("[" + OperationTimes.printDataTime() + "] EXIT [StartJddmGeneralEngine] " + Constant.JddmEngineTypeInfo + "Recv System_Signal ---> ::[SIG" + signal.getName() + "|" + signal.getNumber() + "]-(ctrl+C) Begin Stop Engine ...");
					break;
				case 15:
					System.out.println("[" + OperationTimes.printDataTime() + "] EXIT [StartJddmGeneralEngine] " + Constant.JddmEngineTypeInfo + "Recv System_Signal ---> ::[SIG" + signal.getName() + "|" + signal.getNumber() + "]-(kill -15) Begin Stop Engine ...");
					break;
				default:
					System.out.println("[" + OperationTimes.printDataTime() + "] EXIT [StartJddmGeneralEngine] " + Constant.JddmEngineTypeInfo + "Recv System_Signal ---> ::[SIG" + signal.getName() + "|" + signal.getNumber() + "] Begin Stop Engine ...");
					break;
			}
			String immuTableKeyName = "";
			DataFile dataFile = null;
			//===============================================================================================
			// Total Synchronization Data Operation Processing Before Stopping The (Jddm) Engine
			//===============================================================================================

			if (!GlobalSetConfInfo.IceBergSchemaImmuTableRecordMap.isEmpty()) {
				int maxTableLength = 0;
				int posNo = 0;
				String hivePartitionKeyValue = null;

				System.out.println(" [Stop->JDDM_ENGINE] Jddm Hive Engine (" + ConstantPublic.jddmEngineDataType + ") Data Synchronization Complete .... ");
				for (int i = 0; i < 5; i++) {

					System.out.println(" [Stop->JDDM_ENGINE] Waitting (" + ConstantPublic.jddmEngineDataType + ") Memory Cache Opeation ... ...  ");
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				String[] splitArr = null;
				String tableKeyName = null;
				String parquetFileName = null;
				int loopCount = 0;

				System.out.println(" [Stop->JDDM_ENGINE] GlobalConfInfo.lastDataWriteTimerByParquetMap size:: " + GlobalConfInfo.lastDataWriteTimerByParquetMap.size());
				if (ConstantPublic.jddmEngineDataType.equals("ICEBREG")) {
					try {

						for (Map.Entry<String, Long> cacheTimerMap : GlobalConfInfo.lastDataWriteTimerByParquetMap.entrySet()) {

							System.out.println(" [StopJddmEngine] Jddm Hive Engine [ICEBERG] =====>>> S&T ::" + cacheTimerMap.getKey() + " IceBreg Write  -> LastModifyTimer :: " + transferLongToDate("yyyy-MM-dd HH:mm:ss", cacheTimerMap.getValue()) + " > " + Constant.hiveDiffTimers + " ... ... ");

							splitArr = cacheTimerMap.getKey().split("[.]");
							immuTableKeyName = cacheTimerMap.getKey();

							tableKeyName = splitArr[0] + "." + splitArr[1];
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

								System.out.println(" [StopJddmEngine] Jddm Hive Engine [ICEBERG] =====>>> " + "ThreadID Key ::" + cacheTimerMap.getKey() + " File ::" + filepath + " Count::" + GlobalSetConfInfo.IceBergSchemaImmuTableRecordMap.get(immuTableKeyName).build().size() + " ... ");
								//log.info("----------------> Timer WriteSize ::"+GlobalSetConfInfo.IceBergSchemaImmuTableRecordMap.get(cacheTimerMap.getKey()).build().size());
								for (GenericRecord record : GlobalSetConfInfo.IceBergSchemaImmuTableRecordMap.get(cacheTimerMap.getKey()).build()) {

									dataWriter.write(record);
								}

							} finally {
								dataWriter.close();

							}

							// 3. 将文件写入table中
							dataFile = dataWriter.toDataFile();
							GlobalSetConfInfo.IceBergCacheTableMap.get(tableKeyName).newAppend().appendFile(dataFile).commit();

							GlobalSetConfInfo.IceBergTableGnericCacheMap.remove(immuTableKeyName);
							GlobalSetConfInfo.IceBergSchemaImmuTableRecordMap.remove(immuTableKeyName);

							GlobalConfInfo.lastDataWriteTimerByParquetMap.remove(cacheTimerMap.getKey());


						}
					} catch (Exception ex) {
						ex.printStackTrace();
					} finally {
						dataFile = null;
					}


				}
			}
		}
	}
	/***
     * long 转换成 日期 再转换成String类型
     */
    public static String transferLongToDate(String dateFormat, Long millSec) {
        SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
        Date date = new Date(millSec);
        return sdf.format(date);
    }

}
