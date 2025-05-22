package com.jddm.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

/**
 * className: HdfsValidator<br>
 * description: hdfs connection test<br>
 * author: wjl<br>
 * date: 2025/5/21 14:42<br>
 */
class HdfsValidator {
    public static void validateConnection(String fsDefaultFS) throws Exception {
        Logger log = LogManager.getLogger(HdfsValidator.class);
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", fsDefaultFS);
            FileSystem fs = FileSystem.get(conf);
            fs.getStatus(); // 简单操作测试连接
            log.info("HDFS connection validated successfully");
        } catch (IOException e) {
            throw new Exception("HDFS connection failed", e);
        }
    }
}

