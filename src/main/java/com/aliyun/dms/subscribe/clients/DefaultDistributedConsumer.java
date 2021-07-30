package com.aliyun.dms.subscribe.clients;

import com.aliyun.dts.subscribe.clients.ConsumerContext;
import com.aliyun.dts.subscribe.clients.DTSConsumer;
import com.aliyun.dts.subscribe.clients.common.Checkpoint;
import com.aliyun.dts.subscribe.clients.common.RecordListener;
import com.aliyun.dts.subscribe.clients.metastore.MetaStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DefaultDistributedConsumer implements DistributedConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultDistributedConsumer.class);
    private List<DTSConsumer> dtsConsumers = new ArrayList<>();


    private int corePoolSize = 8;
    private int maximumPoolSize = 8;
    private ThreadPoolExecutor executor;
    private volatile boolean isClosePoolExecutor = false;

    public DefaultDistributedConsumer() {}

    public void addDTSConsumer(DTSConsumer consumer) {
        dtsConsumers.add(consumer);
    }

    public void init(Map<String, String> topic2checkpoint, String dProxy, List<String> sid, String username, String password,
        ConsumerContext.ConsumerSubscribeMode subscribeMode,
        MetaStore<Checkpoint> userRegisteredStore, Map<String, RecordListener> recordListeners) {

        init(topic2checkpoint, dProxy, sid, username, password, subscribeMode, false,
            userRegisteredStore, recordListeners);
    }
    public void init(Map<String, String> topic2checkpoint, String dProxy, List<String> sids, String username, String password,
                     ConsumerContext.ConsumerSubscribeMode subscribeMode, boolean isForceUseInitCheckpoint,
        MetaStore<Checkpoint> userRegisteredStore, Map<String, RecordListener> recordListeners) {

        this.executor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, 1000 * 60,
            TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        int i = 0;
        for (Map.Entry<String, String> topicCheckpoint: topic2checkpoint.entrySet()) {

         //   String brokerUrl = Util.getBrokerFromDProxy(dProxy, topicCheckpoint.getKey());
            ConsumerContext consumerContext = new ConsumerContext(dProxy, topicCheckpoint.getKey(), sids.get(i), username, password,
                    topicCheckpoint.getValue(), subscribeMode);
            consumerContext.setUserRegisteredStore(userRegisteredStore);
            consumerContext.setForceUseCheckpoint(isForceUseInitCheckpoint);

            DTSConsumer dtsConsumer = new DistributedDTSConsumer(consumerContext);
            dtsConsumer.addRecordListeners(recordListeners);

            addDTSConsumer(dtsConsumer);
        }
    }
    @Override
    public void start() {
            for (DTSConsumer consumer: dtsConsumers) {
                try {
                    executor.submit(consumer::start);
                } catch (Exception e) {
                    LOG.error("error starting consumer:" + e);
                }
            }

    }

    public void shutdownGracefully(long timeout, TimeUnit timeUnit) {
        executor.shutdown();

        try {
            if (!executor.awaitTermination(timeout, timeUnit)) {
                executor.shutdownNow();

            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        } finally {
            isClosePoolExecutor = true;
        }
    }

    @Override
    public void addRecordListeners(Map<String, RecordListener> recordListeners) {
        for (DTSConsumer dtsConsumer: dtsConsumers) {
            dtsConsumer.addRecordListeners(recordListeners);
        }
    }

}
