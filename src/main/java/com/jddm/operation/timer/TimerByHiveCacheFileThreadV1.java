package com.jddm.operation.timer;

import com.jddm.common.Constant;
import com.jddm.conf.GlobalConfInfo;
import com.jddm.conf.GlobalSetConfInfo;
import com.jddm.vo.RowOperation;
import com.jddm.operation.IceBergBatchOperationHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.*;

import static com.jddm.operation.IceBergBatchOperationHandler.flushAllThreadsForTable;

/**
 * Timer: scan idle tables, flush cached ops to Iceberg.
 */
public class TimerByHiveCacheFileThreadV1 implements Runnable {

    private static final Logger log = LogManager.getLogger(TimerByHiveCacheFileThreadV1.class);

    @Override
    public void run() {
        log.info("[tid={}] =====>heartBeat.....{}", Thread.currentThread().getId(), GlobalConfInfo.lastDataWriteTimerByParquetMap.size());
        try {
            getIceBergHiveCacheFiles();
        } catch (Exception e) {
            log.error("[TimerFlush][tid={}] Timer Commit Cache File Exception : {}", Thread.currentThread().getId(), e.getMessage(), e);
        }
    }

    /*
        public static void getIceBergHiveCacheFiles() throws Exception {
            Set<String> tablesToFlush = new HashSet<>();
            for (Map.Entry<String, Long> entry :
                    GlobalConfInfo.lastDataWriteTimerByParquetMap.entrySet()) {

                String immuTableKeyName = entry.getKey();

                // 判断是否超过静默阈值
                long idleSeconds = (System.currentTimeMillis() - entry.getValue()) / 1000;
                if (idleSeconds <= Constant.hiveDiffTimers) {
                    continue;
                }

                // 确认 Iceberg 表存在
                if (!GlobalSetConfInfo.IceBergSchemaImmuTableOpsMap.containsKey(immuTableKeyName)
                        || GlobalSetConfInfo.IceBergSchemaImmuTableOpsMap.get(immuTableKeyName).isEmpty()) {
                    continue;
                }

                String[] splitArr   = immuTableKeyName.split("[.]");
                String tableKeyName = splitArr[0] + "." + splitArr[1];

                log.info("[TimerFlush][tid={}] 触发定时 flush. key={} idleSeconds={} lastWriteTime={}", Thread.currentThread().getId(), immuTableKeyName, idleSeconds, transferLongToDate("yyyy-MM-dd HH:mm:ss", entry.getValue()));

                try {
                    // ===== 取出有序操作列表并 flush =====
                    List<RowOperation> orderedOps =
                            GlobalSetConfInfo.IceBergSchemaImmuTableOpsMap.get(immuTableKeyName);

                    String[] split = entry.getKey().split("[.]");
                    tablesToFlush.add(split[0] + "." + split[1]); // 只取 db.table，去掉线程ID

                    // ===== 清理缓存 =====
                    GlobalSetConfInfo.IceBergSchemaImmuTableOpsMap.remove(immuTableKeyName);
                    GlobalSetConfInfo.IceBergTableGnericCacheMap.remove(immuTableKeyName);
                    GlobalSetConfInfo.IceBergSchemaImmuTableRecordMap.remove(immuTableKeyName);
                    GlobalConfInfo.lastDataWriteTimerByParquetMap.remove(immuTableKeyName);

                    log.info("[TimerFlush][tid={}] 定时 flush 完成. key={}", Thread.currentThread().getId(), immuTableKeyName);

                } catch (Exception ex) {
                    log.error("[TimerFlush][tid={}] 定时 flush 异常. key={}", Thread.currentThread().getId(), immuTableKeyName, ex);
                    // 定时器里不能让异常中断整个循环，此处记录即可，下次定时器继续重试
                    ex.printStackTrace();
                }
            }
            for (String tableKeyName : tablesToFlush) {
                flushAllThreadsForTable(tableKeyName); // ← 跨线程合并 flush
            }
        }

        public static String transferLongToDate(String dateFormat, Long millSec) {
            SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
            return sdf.format(new Date(millSec));
        }
    }*/
    public static void getIceBergHiveCacheFiles() throws Exception {
        Set<String> tablesToFlush = new HashSet<>();

        for (Map.Entry<String, Long> entry :
                GlobalConfInfo.lastDataWriteTimerByParquetMap.entrySet()) {

            String immuTableKeyName = entry.getKey();
            long idleSeconds = (System.currentTimeMillis() - entry.getValue()) / 1000;
            if (idleSeconds <= Constant.hiveDiffTimers) {
                continue;
            }

            // LinkedBlockingQueue.isEmpty() 是线程安全的，可直接用于检查该线程分区是否有待 flush 数据
            if (!GlobalSetConfInfo.IceBergSchemaImmuTableOpsMap.containsKey(immuTableKeyName)
                    || GlobalSetConfInfo.IceBergSchemaImmuTableOpsMap.get(immuTableKeyName).isEmpty()) {
                continue;
            }

            String[] split = entry.getKey().split("[.]");
            tablesToFlush.add(split[0] + "." + split[1]);
            // ← 这里什么都不删，数据留着给 flushAllThreadsForTable 用
        }

        // flush 完成后，flushAllThreadsForTable 内部负责清理缓存
        for (String tableKeyName : tablesToFlush) {
            log.info("[TimerFlush][tid={}] trigger flush table={}", Thread.currentThread().getId(), tableKeyName);
            flushAllThreadsForTable(tableKeyName);
            log.info("[TimerFlush][tid={}] flush done table={}", Thread.currentThread().getId(), tableKeyName);
        }
    }
}