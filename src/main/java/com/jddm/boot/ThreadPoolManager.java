package com.jddm.boot;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * className: ThreadPoolManager<br>
 * description: <br>
 * author: wjl<br>
 * date: 2025/5/21 14:47<br>
 */
class ThreadPoolManager {
    private int corePoolSize = 50;
    private int maxPoolSize = 100;

    public static ThreadPoolManager create() {
        return new ThreadPoolManager();
    }

    public ThreadPoolManager withCorePoolSize(int size) {
        this.corePoolSize = size;
        return this;
    }

    public ExecutorService build() {
        return Executors.newFixedThreadPool(corePoolSize);
    }
}

