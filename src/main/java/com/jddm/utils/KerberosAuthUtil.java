package com.jddm.utils;

import com.jddm.common.Constant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.lang.reflect.Method;
import java.net.MalformedURLException;

/**
 * Kerberos 认证工具类。
 *
 * <p>所有方法内部统一判断 {@link Constant#kerberosEnabled} 开关：
 * <ul>
 *   <li>开关=false：方法静默跳过，返回普通空配置，调用方无感知。</li>
 *   <li>开关=true：执行完整 Kerberos 流程。</li>
 * </ul>
 *
 * <p>调用方只需直接调用，无需在外部再加 if/else 判断。
 */
public class KerberosAuthUtil {

    private static final Logger log = LogManager.getLogger(KerberosAuthUtil.class);

    private KerberosAuthUtil() {}

    // -------------------------------------------------------------------------
    // 构建 Hadoop Configuration
    // -------------------------------------------------------------------------

    /**
     * 构建 Hadoop Configuration。
     * <ul>
     *   <li>Kerberos 关闭 → 返回普通空 Configuration（与原有行为完全一致）</li>
     *   <li>Kerberos 开启 → 注入认证相关属性</li>
     * </ul>
     */
    public static Configuration buildHadoopConf() {
        Configuration conf = new Configuration();

        if (!Constant.kerberosEnabled) {
            return conf;
        }

        // 核心修正 1：对标成功代码，只加载项目本地 config 目录下的干净配置，避开 /etc 下的错误配置
        String configPath = Constant.basicWorkPath + File.separator + "config" + File.separator;

        try {
            File coreFile = new File(configPath + "core-site.xml");
            if (coreFile.exists()) {
                conf.addResource(coreFile.toURI().toURL());
            }

            File hdfsFile = new File(configPath + "hdfs-site.xml");
            if (hdfsFile.exists()) {
                conf.addResource(hdfsFile.toURI().toURL());
            }

            File hiveFile = new File(configPath + "hive-site.xml");
            if (hiveFile.exists()) {
                conf.addResource(hiveFile.toURI().toURL());
            }
        } catch (Exception e) {
            log.error("[Kerberos] Error loading local Hadoop XML configs", e);
        }

        conf.set("fs.hdfs.impl.disable.cache", "true");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        log.info("[Kerberos] HadoopConf loaded from local path: {}", configPath);
        return conf;
    }

    // -------------------------------------------------------------------------
    // Keytab 登录（全局只调用一次）
    // -------------------------------------------------------------------------

    /**
     * 使用 Keytab 执行 Kerberos 登录。
     * <p>Kerberos 关闭时静默跳过，无任何副作用。</p>
     * <p>应在引擎启动阶段调用一次，UGI 全局生效。</p>
     */
    public static void loginFromKeytab() throws Exception {
        if (!Constant.kerberosEnabled) {
            log.info("[Kerberos] disabled, skip keytab login");
            return;
        }

        String krb5Path = Constant.krb5ConfFilePath.isEmpty()
                ? Constant.basicWorkPath + File.separator + "config" + File.separator + "krb5.conf"
                : Constant.krb5ConfFilePath;

        // 核心修正 2：删除所有反射和强制写死 Realm 的代码，原汁原味交由底层解析
        System.setProperty("java.security.krb5.conf", krb5Path);
        log.info("[Kerberos] using krb5.conf: {}", krb5Path);

        Configuration conf = buildHadoopConf();
        UserGroupInformation.setConfiguration(conf);

        // 核心修正 3：对标成功代码，使用 ZK_SECURITY_PRINCIPAL_INSTANCE (hive/hdp1) 而不是完整 principal
        String principalToUse = (Constant.hdfsNamenodePrincipal != null && !Constant.hdfsNamenodePrincipal.isEmpty())
                ? Constant.hdfsNamenodePrincipal
                : Constant.kerberosPrincipal;

        UserGroupInformation.loginUserFromKeytab(principalToUse, Constant.kerberosKeytabPath);

        log.info("[Kerberos] login success, currentUser={}", UserGroupInformation.getCurrentUser().getUserName());
    }

    // -------------------------------------------------------------------------
    // TGT 续约（定时调用）
    // -------------------------------------------------------------------------

    /**
     * 检查并续约 TGT。
     * <p>Kerberos 关闭时静默跳过。建议每小时调用一次。</p>
     */
    public static void renewTgtIfNeeded() {
        if (!Constant.kerberosEnabled) {
            // 开关关闭，不打任何日志，避免每小时刷无意义的行
            return;
        }
        try {
            UserGroupInformation ugi = UserGroupInformation.getLoginUser();
            if (ugi != null && ugi.isFromKeytab()) {
                ugi.checkTGTAndReloginFromKeytab();
                log.info("[Kerberos] TGT renewed, user={}", ugi.getUserName());
            }
        } catch (Exception e) {
            log.error("[Kerberos] TGT renewal failed, will retry next cycle", e);
            // 续约失败不抛出，下个周期继续重试，避免中断定时器线程
        }
    }

    private static String getRealm() {
        // 从 principal 中截取 @后面的 realm，例如 hive/hdp1@DSG_TEST.COM → DSG_TEST.COM
        String principal = Constant.kerberosPrincipal;
        int atIdx = principal.indexOf('@');
        return atIdx >= 0 ? principal.substring(atIdx + 1) : "";
    }
}