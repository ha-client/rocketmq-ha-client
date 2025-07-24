package org.apache.rocketmq.ha.client;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class DefaultMQHaProducer extends DefaultMQProducer {
    private final static Logger log = LoggerFactory.getLogger(DefaultMQHaProducer.class);
    // fields can copy from DefaultMQProducer
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
        }
    };
    // all producers for rocketmq clusters
    private final static LinkedHashMap<String, DefaultMQProducer> PRODUCERS = new LinkedHashMap<>();

    // copy value from source instance to target instance
    static void copyFieldValue(String fieldName, Class<?> sourceInstanceClass, Object sourceInstance, Object targetInstance) {
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

    // change a normal producer to ha producer by user
    public static DefaultMQHaProducer changeToHa(DefaultMQProducer notStartedProducer, LinkedHashMap<String, NameserverInfo> namesrvLists) {
        DefaultMQHaProducer haProducer = new DefaultMQHaProducer();
        String prefixInstanceName = UtilAll.getPid() + "#" + System.nanoTime();
        for (String key : namesrvLists.keySet()) {
            DefaultMQProducer p = new DefaultMQProducer(
                    notStartedProducer.getProducerGroup(),
                    new AclClientRPCHook(new SessionCredentials(namesrvLists.get(key).getAccessKey(), namesrvLists.get(key).getSecretKey())),
                    notStartedProducer.isEnableTrace(),
                    notStartedProducer.getTraceTopic()
            );
            p.setNamesrvAddr(namesrvLists.get(key).getAddressList());

            for (String fieldName : FIELDS_CAN_COPY) {
                copyFieldValue(fieldName, DefaultMQProducer.class, notStartedProducer, p);
            }
            p.setInstanceName(prefixInstanceName + "#" + key + "#ha");
            PRODUCERS.put(key, p);
        }
        return haProducer;
    }

    // add field to the copy list
    static void addCopyField(String fieldName) {
        FIELDS_CAN_COPY.add(fieldName);
    }

    // start all producers in the ha producer
    @Override
    public void start() throws MQClientException {
        if (PRODUCERS.isEmpty()) {
            throw new RuntimeException("no producer added");
        }
        for (String key : PRODUCERS.keySet()) {
            PRODUCERS.get(key).start();
            log.info("start producer[" + key + "] success");
        }
    }

    // shutdown all producers in the ha producer and clean the producer map
    @Override
    public void shutdown() {
        for (String key : PRODUCERS.keySet()) {
            try {
                PRODUCERS.get(key).shutdown();
                log.info("shutdown producer[" + key + "] success");
            } catch (Exception ex) {
                log.error("shutdown producer[" + key + "] throw exception", ex);
            }
        }

        PRODUCERS.clear();
    }

    // send message with the ha producer
    @Override
    public SendResult send(Message msg) {
        SendResult sr = null;
        String topic = msg.getTopic();
        for (String key : PRODUCERS.keySet()) {
            try {
                sr = PRODUCERS.get(key).send(msg);
            } catch (Exception ex) {
                log.error("send message to " + msg.getTopic() + " using producer[" + key + "] throw exception", ex);
                msg.setTopic(topic); // RCODE001: reset topic. producer would update topic with its namespace
                continue;
            }
            if (sr.getSendStatus() == SendStatus.SEND_OK) {
                break;
            }
        }

        if (sr == null) {
            throw new RuntimeException("fail to send message to topic[" + msg.getTopic() + "]");
        }
        return sr;
    }

    // name server info
    public static class NameserverInfo {
        private String addressList;
        private String accessKey;
        private String secretKey;

        public NameserverInfo(String addressList, String accessKey, String secretKey) {
            this.addressList = addressList;
            this.accessKey = accessKey;
            this.secretKey = secretKey;
        }

        public String getAddressList() {
            return addressList;
        }

        public void setAddressList(String addressList) {
            this.addressList = addressList;
        }

        public String getAccessKey() {
            return accessKey;
        }

        public void setAccessKey(String accessKey) {
            this.accessKey = accessKey;
        }

        public String getSecretKey() {
            return secretKey;
        }

        public void setSecretKey(String secretKey) {
            this.secretKey = secretKey;
        }
    }
}