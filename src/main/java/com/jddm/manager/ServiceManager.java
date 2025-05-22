package com.jddm.manager;

import com.publics.engine.operation.socketSecGeneration.SocketGeneralEngine;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * className: ServiceManager<br>
 * description: <br>
 * author: wjl<br>
 * date: 2025/5/22 14:46<br>
 */

public class ServiceManager {
    public ExecutorService threadPool;
    public ScheduledExecutorService scheduledServices;
    public SocketGeneralEngine socketEngine;
    public Logger log = LogManager.getLogger(ServiceManager.class);
    void shutdown() {
        try {
            if (socketEngine != null) socketEngine.close();
            if (threadPool != null) threadPool.shutdownNow();
            if (scheduledServices != null) scheduledServices.shutdownNow();
        } catch (Exception e) {
            log.error("Error during service shutdown", e);
        }
    }
}
