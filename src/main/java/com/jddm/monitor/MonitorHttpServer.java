package com.jddm.monitor;

import com.jddm.conf.GlobalConfInfo;
import com.jddm.conf.GlobalSetConfInfo;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 引擎内置轻量级 HTTP 监控服务器。
 *
 * 使用 JDK 8 自带的 {@code com.sun.net.httpserver.HttpServer}，无需新增任何 Maven 依赖。
 *
 * 对外暴露两个端点：
 *   GET /            — 返回 classpath 下的 Monitor.html（src/main/resources/Monitor.html）
 *   GET /api/metrics — 返回 JSON 监控数据，字段与 Monitor.html 前端 JS 完全对应
 *
 * 启动方式：在 StartIcebergEngine#initializeServices() 的末尾加一行：
 *   MonitorHttpServer.start(8080);
 */
public class MonitorHttpServer {

    private static final Logger log = LogManager.getLogger(MonitorHttpServer.class);

    // Monitor.html 里用 HH:mm:ss 格式显示时间戳
    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss");

    // 用于计算 throughputPerSec：记录上次采样时的总行数和时间
    private static final AtomicLong lastTotalRows    = new AtomicLong(0L);
    private static final AtomicLong lastSampleTimeMs = new AtomicLong(System.currentTimeMillis());

    private MonitorHttpServer() {}

    /**
     * 启动监控 HTTP 服务，绑定指定端口，后台运行，不阻塞引擎主流程。
     *
     * @param port 监听端口，建议 8080
     */
    public static void start(int port) {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/api/metrics", new MetricsHandler());
            server.createContext("/",            new StaticFileHandler());
            // 独立线程池，与引擎工作线程完全隔离
            server.setExecutor(Executors.newFixedThreadPool(2));
            server.start();
            log.info("[Monitor] 监控服务已启动，端口 {}，访问 http://localhost:{}/", port, port);
        } catch (IOException e) {
            // 监控服务失败不影响引擎正常运行，仅记录日志
            log.error("[Monitor] HTTP 服务启动失败，端口 {}：{}", port, e.getMessage(), e);
        }
    }

    // =========================================================================
    // /api/metrics 处理器
    // =========================================================================

    private static class MetricsHandler implements HttpHandler {

        private final com.sun.management.OperatingSystemMXBean osMXBean =
                (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        private final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendText(exchange, 405, "Method Not Allowed");
                return;
            }
            String json = buildJson();
            exchange.getResponseHeaders().set("Content-Type",  "application/json; charset=UTF-8");
            exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
            sendText(exchange, 200, json);
        }

        private String buildJson() {

            // ---- 系统指标 ----
            Runtime rt        = Runtime.getRuntime();
            long maxMemMb     = rt.maxMemory()   / 1024 / 1024;
            long usedMemMb    = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;
            double cpuLoad    = osMXBean.getSystemCpuLoad();
            // getSystemCpuLoad() 在冷启动时返回 -1，兜底为 0
            double cpuPercent = cpuLoad < 0 ? 0.0 : cpuLoad * 100;
            int activeThreads = threadMXBean.getThreadCount();

            // ---- 引擎指标 ----
            int queueSize = GlobalSetConfInfo.icebergEngineOperationQueue.size();

            // 累计已提交行数：从 cumulativeRowsByTableMap 读取（只增不减，flush 后不清零）
            // 再加上当前批次尚未 flush 的行数（仍在 engineAtomicByTableKeyMap 中）
            long totalRows = 0L;
            for (AtomicLong v : GlobalConfInfo.cumulativeRowsByTableMap.values()) {
                totalRows += v.get();
            }
            // 加上当前批次还没提交的行数，让数字实时增长而不是等 flush 才跳一下
            for (java.util.concurrent.atomic.AtomicInteger v
                    : GlobalConfInfo.engineAtomicByTableKeyMap.values()) {
                totalRows += v.get();
            }

            // 计算吞吐量（行/秒）：本次总行数与上次采样的差值 / 间隔秒数
            long now        = System.currentTimeMillis();
            long prevTotal  = lastTotalRows.getAndSet(totalRows);
            long prevTimeMs = lastSampleTimeMs.getAndSet(now);
            long elapsedMs  = now - prevTimeMs;
            long throughput = elapsedMs > 0 ? (totalRows - prevTotal) * 1000L / elapsedMs : 0L;
            if (throughput < 0) throughput = 0;

            // ---- 各表详情（按 db.table 聚合）----
            Map<String, Long>   tableRows      = new TreeMap<>();
            Map<String, String> tableLastWrite = new TreeMap<>();

            // 先把累计已提交的行数填入（cumulativeRowsByTableMap 的 key 已经是 db.table 格式）
            for (Map.Entry<String, AtomicLong> e : GlobalConfInfo.cumulativeRowsByTableMap.entrySet()) {
                tableRows.merge(e.getKey(), e.getValue().get(), Long::sum);
            }
            // 再叠加当前批次未提交的行数
            for (Map.Entry<String, java.util.concurrent.atomic.AtomicInteger> e
                    : GlobalConfInfo.engineAtomicByTableKeyMap.entrySet()) {
                String tableKey = toTableKey(e.getKey());
                tableRows.merge(tableKey, (long) e.getValue().get(), Long::sum);
            }

            // 聚合最后写入时间：取同一张表各线程中时间戳最新的那条
            // lastDataWriteTimerByParquetMap 的 key 同样是 db.table.threadId
            Map<String, Long> latestTsPerTable = new TreeMap<>();
            for (Map.Entry<String, Long> e : GlobalConfInfo.lastDataWriteTimerByParquetMap.entrySet()) {
                String tableKey = toTableKey(e.getKey());
                latestTsPerTable.merge(tableKey, e.getValue(),
                        (existing, incoming) -> incoming > existing ? incoming : existing);
            }
            for (Map.Entry<String, Long> e : latestTsPerTable.entrySet()) {
                long diffSec   = (System.currentTimeMillis() - e.getValue()) / 1000;
                String timeStr = LocalDateTime.now().minusSeconds(diffSec).format(TIME_FMT);
                tableLastWrite.put(e.getKey(), timeStr);
            }

            // ---- 手工拼 JSON（不引入 fastjson，避免循环依赖）----
            StringBuilder sb = new StringBuilder(512);
            sb.append("{\n");
            sb.append("  \"timestamp\": \"").append(LocalDateTime.now().format(TIME_FMT)).append("\",\n");

            // system 块 —— 字段名与 Monitor.html JS 里的 data.system.xxx 严格对应
            sb.append("  \"system\": {\n");
            sb.append("    \"cpuUsage\": ").append(String.format("%.1f", cpuPercent)).append(",\n");
            sb.append("    \"memoryUsedMb\": ").append(usedMemMb).append(",\n");
            sb.append("    \"memoryMaxMb\": ").append(maxMemMb).append(",\n");
            sb.append("    \"activeThreads\": ").append(activeThreads).append("\n");
            sb.append("  },\n");

            // engine 块 —— 对应 data.engine.xxx
            sb.append("  \"engine\": {\n");
            sb.append("    \"queueSize\": ").append(queueSize).append(",\n");
            sb.append("    \"throughputPerSec\": ").append(throughput).append(",\n");
            sb.append("    \"totalRowsProcessed\": ").append(totalRows).append("\n");
            sb.append("  },\n");

            // tables 数组 —— 对应 data.tables[].tableName / rowsProcessed / lastWriteTime
            sb.append("  \"tables\": [\n");
            boolean first = true;
            for (Map.Entry<String, Long> e : tableRows.entrySet()) {
                if (!first) sb.append(",\n");
                first = false;
                String lastWrite = tableLastWrite.getOrDefault(e.getKey(), "N/A");
                sb.append("    {")
                        .append("\"tableName\": \"").append(escJson(e.getKey())).append("\", ")
                        .append("\"rowsProcessed\": ").append(e.getValue()).append(", ")
                        .append("\"lastWriteTime\": \"").append(lastWrite).append("\"")
                        .append("}");
            }
            sb.append("\n  ]\n}");
            return sb.toString();
        }

        /**
         * 将 "db.table.threadId" 格式的 key 截取为 "db.table"。
         * 只有一个点的 key（本身已是 db.table 格式）直接返回。
         */
        private static String toTableKey(String key) {
            if (key == null) return "unknown";
            int first = key.indexOf('.');
            if (first < 0) return key;
            int second = key.indexOf('.', first + 1);
            return second > 0 ? key.substring(0, second) : key;
        }

        private static String escJson(String s) {
            return s == null ? "" : s.replace("\\", "\\\\").replace("\"", "\\\"");
        }
    }

    // =========================================================================
    // 静态文件处理器：返回 Monitor.html
    // =========================================================================

    private static class StaticFileHandler implements HttpHandler {

        // Monitor.html 内容只需从 classpath 加载一次，后续复用字节数组，减少 IO
        private volatile byte[] cachedHtml = null;

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String path = exchange.getRequestURI().getPath();
            // 只处理根路径和 monitor.html，其他路径一律 404
            if (!"/".equals(path) && !path.toLowerCase().endsWith("monitor.html")) {
                sendText(exchange, 404, "Not Found");
                return;
            }
            byte[] body = getHtmlBytes();
            exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        }

        private byte[] getHtmlBytes() throws IOException {
            if (cachedHtml != null) return cachedHtml;
            // 从 classpath 根路径读取，对应 src/main/resources/Monitor.html
            try (InputStream is = getClass().getClassLoader().getResourceAsStream("Monitor.html")) {
                if (is == null) {
                    String tip = "<h2>Monitor.html not found. "
                            + "Please place it at src/main/resources/Monitor.html</h2>";
                    return tip.getBytes(StandardCharsets.UTF_8);
                }
                // Java 8 兼容写法，readAllBytes() 是 Java 9+ 才有的
                ByteArrayOutputStream buf = new ByteArrayOutputStream();
                byte[] block = new byte[4096];
                int n;
                while ((n = is.read(block)) != -1) {
                    buf.write(block, 0, n);
                }
                cachedHtml = buf.toByteArray();
                return cachedHtml;
            }
        }
    }

    // =========================================================================
    // 工具方法
    // =========================================================================

    private static void sendText(HttpExchange exchange, int code, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}