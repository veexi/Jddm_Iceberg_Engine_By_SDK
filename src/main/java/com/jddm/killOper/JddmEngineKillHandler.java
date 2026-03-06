package com.jddm.killOper;

import com.jddm.common.Constant;
import com.jddm.conf.GlobalConfInfo;
import com.jddm.conf.GlobalSetConfInfo;
import com.jddm.operation.IceBergBatchOperationHandler;
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
import java.util.*;
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
                    System.out.println("[" + OperationTimes.printDataTime() + "] EXIT [StartJddmIcebergEngine] " + Constant.JddmEngineTypeInfo + "Recv System_Signal ---> ::[SIG" + signal.getName() + "|" + signal.getNumber() + "]-(ctrl+C) Begin Stop Engine ...");
                    break;
                case 15:
                    System.out.println("[" + OperationTimes.printDataTime() + "] EXIT [StartJddmIcebergEngine] " + Constant.JddmEngineTypeInfo + "Recv System_Signal ---> ::[SIG" + signal.getName() + "|" + signal.getNumber() + "]-(kill -15) Begin Stop Engine ...");
                    break;
                default:
                    System.out.println("[" + OperationTimes.printDataTime() + "] EXIT [StartJddmIcebergEngine] " + Constant.JddmEngineTypeInfo + "Recv System_Signal ---> ::[SIG" + signal.getName() + "|" + signal.getNumber() + "] Begin Stop Engine ...");
                    break;
            }
            System.out.println(" [StopJddmEngine] OpsMap.size="
                    + GlobalSetConfInfo.IceBergSchemaImmuTableOpsMap.size());
            System.out.println(" [StopJddmEngine] queue.size="
                    + GlobalSetConfInfo.icebergEngineOperationQueue.size());
            System.out.println(" [StopJddmEngine] OpsMap.keys="
                    + GlobalSetConfInfo.IceBergSchemaImmuTableOpsMap.keySet());
            String immuTableKeyName = "";
            DataFile dataFile = null;
            //===============================================================================================
            // Total Synchronization Data Operation Processing Before Stopping The (Jddm) Engine
            //===============================================================================================

            if (!GlobalSetConfInfo.IceBergSchemaImmuTableOpsMap.isEmpty()
                    || !GlobalSetConfInfo.icebergEngineOperationQueue.isEmpty()) {


                while (!GlobalSetConfInfo.icebergEngineOperationQueue.isEmpty()) {
                    System.out.println(" [StopJddmEngine] queue draining, remaining="
                            + GlobalSetConfInfo.icebergEngineOperationQueue.size());
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

                Set<String> tablesToFlush = new HashSet<>();
                for (String key : GlobalSetConfInfo.IceBergSchemaImmuTableOpsMap.keySet()) {
                    if (!GlobalSetConfInfo.IceBergSchemaImmuTableOpsMap.get(key).isEmpty()) {
                        String[] split = key.split("[.]");
                        tablesToFlush.add(split[0] + "." + split[1]);
                    }
                }
                for (String flushTableName : tablesToFlush) {
                    System.out.println(" [StopJddmEngine] force flush table=" + flushTableName);
                    try {
                        IceBergBatchOperationHandler.flushAllThreadsForTable(flushTableName);
                        System.out.println(" [StopJddmEngine] flush done table=" + flushTableName);
                    } catch (Exception ex) {
                        System.out.println(" [StopJddmEngine] flush failed table=" + flushTableName);
                        ex.printStackTrace();
                    }
                }
            }

            try {
                System.out.println("[" + OperationTimes.printDataTime() + "] EXIT [StartJddmIcebergEngine] " + Constant.JddmEngineTypeInfo + "Begin Stop Please Waitting (3)Seconds ... ... ");
                Thread.sleep(300);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            EngineStateInfo.writeEngineStateForJddm("stop", "sucess");
            System.out.println("[" + OperationTimes.printDataTime() + "] EXIT [StartJddmIcebergEngine] " + Constant.JddmEngineTypeInfo + " Stoped Complete ! ");
            System.out.println(" ");
            System.exit(-1);
        }
    }
	/**
     * Convert long timestamp to date string.
     */
    public static String transferLongToDate(String dateFormat, Long millSec) {
        SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
        Date date = new Date(millSec);
        return sdf.format(date);
    }

}
