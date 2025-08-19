package dev.rocketmq.ha.grpc;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.SessionCredentialsProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.ProducerBuilder;
import org.apache.rocketmq.client.apis.producer.RecallReceipt;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.apis.producer.Transaction;
import org.apache.rocketmq.client.apis.producer.TransactionChecker;
import org.apache.rocketmq.shaded.org.slf4j.Logger;
import org.apache.rocketmq.shaded.org.slf4j.LoggerFactory;

public class DefaultMQHaProducer implements Producer {
    private final Logger log = LoggerFactory.getLogger(DefaultMQHaProducer.class);

    private final LinkedHashMap<String, Producer> PRODUCERS = new LinkedHashMap<>();

    public static DefaultMQHaProducer buildProducer(LinkedHashMap<String, EndpointInfo> endpointMap, TransactionChecker checker, String... topics) throws ClientException {
        DefaultMQHaProducer producer = new DefaultMQHaProducer();
        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        for (Map.Entry<String, EndpointInfo> entry : endpointMap.entrySet()) {
            SessionCredentialsProvider sessionCredentialsProvider =
                    new StaticSessionCredentialsProvider(entry.getValue().getAccessKey(), entry.getValue().getSecretKey());
            ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
                    .setEndpoints(entry.getValue().getAddressList())
                    .setCredentialProvider(sessionCredentialsProvider)
                    .build();
            final ProducerBuilder builder = provider.newProducerBuilder()
                    .setClientConfiguration(clientConfiguration)
                    .setTopics(topics);
            if (checker != null) {
                builder.setTransactionChecker(checker);
            }
            producer.PRODUCERS.put(entry.getKey(), builder.build());
        }
        return producer;
    }

    @Override
    public SendReceipt send(Message message) throws ClientException {
        for (String key : PRODUCERS.keySet()) {
            try {
                SendReceipt sr = PRODUCERS.get(key).send(message);
                if (sr != null) {
                    return sr;
                }
            } catch (ClientException e) {
                log.error("send message to producer[" + key + "] throw exception", e);
            }
        }
        throw new ClientException("Failed to send message to any producer");
    }

    @Override
    public SendReceipt send(Message message, Transaction transaction) throws ClientException {
        for (String key : PRODUCERS.keySet()) {
            try {
                SendReceipt sr = PRODUCERS.get(key).send(message, transaction);
                if (sr != null) {
                    return sr;
                }
            } catch (ClientException e) {
                log.error("send transaction message to producer[" + key + "] throw exception", e);
            }
        }
        throw new ClientException("Failed to send transaction message to any producer");
    }

    @Override
    public CompletableFuture<SendReceipt> sendAsync(Message message) {
        for (String key : PRODUCERS.keySet()) {
            return PRODUCERS.get(key).sendAsync(message); // 如果处理失败，用户需要降级到同步发送。 同步发送可以发送到其他集群
        }
        return null;
    }

    @Override
    public Transaction beginTransaction() throws ClientException {
        for (String key : PRODUCERS.keySet()) {
            return PRODUCERS.get(key).beginTransaction();
        }
        return null;
    }

    @Override
    public RecallReceipt recallMessage(String topic, String recallHandle) throws ClientException {
        for (String key : PRODUCERS.keySet()) {
            return PRODUCERS.get(key).recallMessage(topic, recallHandle);
        }
        return null;
    }

    @Override
    public CompletableFuture<RecallReceipt> recallMessageAsync(String topic, String recallHandle) {
        for (String key : PRODUCERS.keySet()) {
            return PRODUCERS.get(key).recallMessageAsync(topic, recallHandle);
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        for (String key : PRODUCERS.keySet()) {
            PRODUCERS.get(key).close();
        }
    }

    public static class EndpointInfo {
        private String addressList;
        private String accessKey;
        private String secretKey;

        public EndpointInfo(String addressList, String accessKey, String secretKey) {
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