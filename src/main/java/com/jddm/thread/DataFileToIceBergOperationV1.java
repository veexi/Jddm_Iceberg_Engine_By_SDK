package com.jddm.thread;

import com.jddm.common.Constant;
import com.jddm.conf.GlobalConfInfo;
import com.jddm.conf.GlobalSetConfInfo;
import com.jddm.vo.RowOperation;
import com.jddm.operation.IceBergBatchOperationHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Flush ops from OpsMap to Iceberg via IceBergBatchOperationHandler.
 */
public class DataFileToIceBergOperationV1 extends Thread {

    public Logger log = LogManager.getLogger(DataFileToIceBergOperationV1.class);

    private String schemaKeyByParquetThreadID;
    private String schemaKeyByParquet;

    @Override
    public void run() {
        if (!Constant.writeToIceBergDBFlag) {
            log.info("[DataFileToIceberg] writeToIceBergDBFlag=false skip threadId={}",
                    Thread.currentThread().getId());
            return;
        }

        Constant.writeToIceBergDBFlag = false;

        String immuTableKeyName = schemaKeyByParquetThreadID;
        String[] splitArr       = immuTableKeyName.split("[.]");
        String tableKeyName     = splitArr[0] + "." + splitArr[1];

        try {
            GlobalSetConfInfo.IceBergOperationBeginMap
                    .putIfAbsent(immuTableKeyName, new AtomicInteger(0));

            List<RowOperation> orderedOps =
                    GlobalSetConfInfo.IceBergSchemaImmuTableOpsMap.get(immuTableKeyName);

            if (orderedOps == null || orderedOps.isEmpty()) {
                log.info("[DataFileToIceberg] ops empty skip key={}", immuTableKeyName);
                return;
            }

            IceBergBatchOperationHandler.flushBatch(immuTableKeyName, tableKeyName, orderedOps);

            GlobalConfInfo.lastDataWriteTimerByParquetMap.put(
                    schemaKeyByParquetThreadID, System.currentTimeMillis());
            GlobalSetConfInfo.IceBergOperationCompleteMap.put(schemaKeyByParquetThreadID, true);

            GlobalSetConfInfo.IceBergSchemaImmuTableOpsMap.remove(immuTableKeyName);
            GlobalSetConfInfo.IceBergTableGnericCacheMap.remove(immuTableKeyName);

            log.info("[DataFileToIceberg] batch done key={} ops={}",
                    immuTableKeyName, orderedOps.size());

        } catch (Exception ex) {
            log.error("[DataFileToIceberg] batch error key={}", immuTableKeyName, ex);
            ex.printStackTrace();
        }
    }

    // ---------- Getter / Setter ----------

    public String getSchemaKeyByParquetThreadID() { return schemaKeyByParquetThreadID; }
    public void setSchemaKeyByParquetThreadID(String key) { this.schemaKeyByParquetThreadID = key; }

    public String getSchemaKeyByParquet() { return schemaKeyByParquet; }
    public void setSchemaKeyByParquet(String key) { this.schemaKeyByParquet = key; }
}