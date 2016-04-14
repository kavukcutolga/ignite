package org.apache.ignite.thread;

import org.apache.ignite.internal.util.worker.GridWorker;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Created by tolga on 14/04/16.
 */
public class IgniteScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor {

    private int fixedRate;
    private Map<String,ScheduledFuture<?>> scheduledFutureMap = new HashMap<>();

    private IgniteScheduledThreadPoolExecutor(int corePoolSize, ThreadFactory factory) {
        super(corePoolSize,factory);
    }

    public void schedule(GridWorker gridWorker, String cacheName){
        ScheduledFuture<?> scheduledFuture = super.scheduleAtFixedRate(
                gridWorker,
                0,this.fixedRate,
                TimeUnit.MILLISECONDS);
        this.scheduledFutureMap.put(cacheName,scheduledFuture);
    }

    public static IgniteScheduledThreadPoolExecutor executor(int corePoolSize,
                                                             int flusherFixedRate){

        IgniteScheduledThreadPoolExecutor executor = new IgniteScheduledThreadPoolExecutor(corePoolSize,
                new IgniteThreadFactory(null))
                .withFixedRate(flusherFixedRate);

        executor.setRemoveOnCancelPolicy(true);

        return executor;
    }

    private IgniteScheduledThreadPoolExecutor withFixedRate(int fixedRate){
        this.setFixedRate(fixedRate);
        return this;
    }


    public void setFixedRate(int fixedRate) {
        this.fixedRate = fixedRate;
    }

    public void unschedule(String cacheName){
        this.scheduledFutureMap.get(cacheName).cancel(true);
    }
}
