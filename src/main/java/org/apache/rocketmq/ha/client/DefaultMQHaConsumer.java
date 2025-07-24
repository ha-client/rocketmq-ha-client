package org.apache.rocketmq.ha.client;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static org.apache.rocketmq.ha.client.DefaultMQHaProducer.copyFieldValue;

public class DefaultMQHaConsumer extends DefaultMQPushConsumer {
    private final static Logger log = LoggerFactory.getLogger(DefaultMQHaConsumer.class);
    // fields can copy from DefaultMQPushConsumer
    private final static List<String> FIELDS_CAN_COPY = new ArrayList<>() {
        {
            add("consumerGroup");
            add("consumeThreadMin");
            add("consumeThreadMax");
            add("consumeConcurrentlyMaxSpan");
            add("pullThresholdForQueue");
            add("popThresholdForQueue");
            add("pullThresholdSizeForQueue");
            add("pullThresholdForTopic");
            add("pullThresholdSizeForTopic");
            add("pullInterval");
            add("consumeMessageBatchMaxSize");
            add("pullBatchSize");
            add("pullBatchSizeInBytes");
            add("postSubscriptionWhenPull");
            add("unitMode");
            add("maxReconsumeTimes");
            add("suspendCurrentQueueTimeMillis");
            add("consumeTimeout");
            add("popInvisibleTime");
            add("popBatchNums");
            add("awaitTerminationMillisWhenShutdown");
            add("clientRebalance");
        }
    };
    // all consumers for rocketmq clusters
    private final static LinkedHashMap<String, DefaultMQPushConsumer> CONSUMERS = new LinkedHashMap<>();

    // add field to the copy list
    static void addCopyField(String fieldName) {
        FIELDS_CAN_COPY.add(fieldName);
    }

    public static DefaultMQHaConsumer changeToHa(DefaultMQPushConsumer notStartedConsumer, LinkedHashMap<String, DefaultMQHaProducer.NameserverInfo> namesrvLists) {
        DefaultMQHaConsumer haConsumer = new DefaultMQHaConsumer();

        String prefixInstanceName = UtilAll.getPid() + "#" + System.nanoTime();

        for (String key : namesrvLists.keySet()) {
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(
                    notStartedConsumer.getConsumerGroup(),
                    new AclClientRPCHook(new SessionCredentials(namesrvLists.get(key).getAccessKey(), namesrvLists.get(key).getSecretKey())),
                    new AllocateMessageQueueAveragely(),
                    true,
                    null
            );
            for (String fieldName : FIELDS_CAN_COPY) {
                copyFieldValue(fieldName, notStartedConsumer.getClass(), notStartedConsumer, consumer);
            }
            consumer.setNamesrvAddr(namesrvLists.get(key).getAddressList());
            consumer.setInstanceName(prefixInstanceName + "#" + key + "#ha");
            CONSUMERS.put(key, consumer);
        }
        return haConsumer;
    }

    @Override
    public void subscribe(String topic, String subExpression) throws MQClientException {
        for (String key : CONSUMERS.keySet()) {
            CONSUMERS.get(key).subscribe(topic, subExpression);
        }
    }

    @Override
    public void unsubscribe(String topic) {
        for (String key : CONSUMERS.keySet()) {
            CONSUMERS.get(key).unsubscribe(topic);
        }
    }

    @Override
    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        for (String key : CONSUMERS.keySet()) {
            CONSUMERS.get(key).setConsumeFromWhere(consumeFromWhere);
        }
    }

    @Override
    public void setConsumeTimestamp(String consumeTimestamp) {
        for (String key : CONSUMERS.keySet()) {
            CONSUMERS.get(key).setConsumeTimestamp(consumeTimestamp);
        }
    }

    @Override
    public void setMessageModel(MessageModel messageModel) {
        for (String key : CONSUMERS.keySet()) {
            CONSUMERS.get(key).setMessageModel(messageModel);
        }
    }

    @Override
    public void registerMessageListener(MessageListenerConcurrently messageListener) {
        for (String key : CONSUMERS.keySet()) {
            CONSUMERS.get(key).registerMessageListener(messageListener);
        }
    }

    @Override
    public void registerMessageListener(MessageListenerOrderly messageListener) {
        for (String key : CONSUMERS.keySet()) {
            CONSUMERS.get(key).registerMessageListener(messageListener);
        }
    }

    // start all consumers in the ha consumer
    @Override
    public void start() throws MQClientException {
        if (CONSUMERS.isEmpty()) {
            throw new RuntimeException("no consumer added");
        }
        for (String key : CONSUMERS.keySet()) {
            CONSUMERS.get(key).start();
            log.info("start consumer[" + key + "] success");
        }
    }

    // shutdown all consumers in the ha consumer and clean the consumer map
    @Override
    public void shutdown() {
        for (String key : CONSUMERS.keySet()) {
            try {
                CONSUMERS.get(key).shutdown();
                log.info("shutdown consumer[" + key + "] success");
            } catch (Exception ex) {
                log.error("shutdown consumer[" + key + "] throw exception", ex);
            }
        }

        CONSUMERS.clear();
    }
}
