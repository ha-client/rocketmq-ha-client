package org.apache.rocketmq.ha.client;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class DefaultMQHaProducer extends DefaultMQProducer {

    private final static List<String> FIELDS_CAN_COPY = new ArrayList<>() {
        {
            add("producerGroup");
            add("topics");
            add("defaultTopicQueueNums");
            add("sendMsgTimeout");
            add("sendMsgMaxTimeoutPerRequest");
            add("compressMsgBodyOverHowmuch");
            add("retryTimesWhenSendFailed");
            add("retryTimesWhenSendAsyncFailed");
            add("retryAnotherBrokerWhenNotStoreOK");
            add("maxMessageSize");
            add("traceDispatcher");
            add("autoBatch");
            add("produceAccumulator");
            add("enableBackpressureForAsyncMode");
            add("backPressureForAsyncSendNum");
            add("backPressureForAsyncSendSize");
            add("batchMaxDelayMs");
            add("batchMaxBytes");
            add("totalBatchMaxBytes");
            add("rpcHook");
        }
    };
    private final static LinkedHashMap<String, DefaultMQProducer> producers = new LinkedHashMap<>();

    private static void copyFieldValue(String fieldName, Class<?> sourceInstanceClass, Object sourceInstance, Object targetInstance) {
        Field field = null;
        try {
            field = sourceInstanceClass.getDeclaredField(fieldName);
            field.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException("get field definition from " + sourceInstanceClass.getName() + " throw exception", e);
        }

        Object fieldValue = null;
        try {
            fieldValue = field.get(sourceInstance);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("get fieldValue from " + sourceInstanceClass.getName() + " throw exception", e);
        }

        // set field value to target instance
        if (fieldValue != null) {
            try {
                field.set(targetInstance, fieldValue);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("set fieldValue to " + targetInstance.toString() + " throw exception", e);
            }
        }
    }

    public static DefaultMQHaProducer changeToHa(DefaultMQProducer notStartedProducer, LinkedHashMap<String, String> namesrvLists) {
        DefaultMQHaProducer haProducer = new DefaultMQHaProducer();

        for (String key : namesrvLists.keySet()) {
            DefaultMQProducer p = new DefaultMQProducer(
                    notStartedProducer.getProducerGroup(),
                    null,
                    notStartedProducer.isEnableTrace(),
                    notStartedProducer.getTraceTopic()
            );
            p.setNamesrvAddr(namesrvLists.get(key));
            for (String fieldName : FIELDS_CAN_COPY) {
                copyFieldValue(fieldName, DefaultMQProducer.class, notStartedProducer, p);
            }
            p.setUnitName(key);
        }
        return haProducer;
    }

    public static void addCopyField(String fieldName) {
        FIELDS_CAN_COPY.add(fieldName);
    }

    @Override
    public void start() throws MQClientException {
        for (String key : producers.keySet()) {
            producers.get(key).start();
        }
    }

    @Override
    public void shutdown() {
        for (String key : producers.keySet()) {
            producers.get(key).shutdown();
        }
    }

    @Override
    public SendResult send(Message msg) {
        SendResult sr = null;
        for (String key : producers.keySet()) {
            try {
                sr = producers.get(key).send(msg);
            } catch (Exception ex) {
                continue;
            }
            if (sr.getSendStatus() != SendStatus.SEND_OK) {
                continue;
            }
        }

        if (sr == null) {
            throw new RuntimeException("send message failed");
        }
        return sr;
    }
}