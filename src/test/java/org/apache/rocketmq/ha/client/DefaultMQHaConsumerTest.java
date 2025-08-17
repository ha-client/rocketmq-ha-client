package org.apache.rocketmq.ha.client;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.junit.Test;

import static org.apache.rocketmq.ha.client.DefaultMQHaProducerTest.NAMESRV_LISTS1;
import static org.apache.rocketmq.ha.client.DefaultMQHaProducerTest.NAMESRV_LISTS1_AK;
import static org.apache.rocketmq.ha.client.DefaultMQHaProducerTest.NAMESRV_LISTS1_SK;
import static org.apache.rocketmq.ha.client.DefaultMQHaProducerTest.NAMESRV_LISTS2;
import static org.apache.rocketmq.ha.client.DefaultMQHaProducerTest.NAMESRV_LISTS2_AK;
import static org.apache.rocketmq.ha.client.DefaultMQHaProducerTest.NAMESRV_LISTS2_SK;
import static org.apache.rocketmq.ha.client.DefaultMQHaProducerTest.TOPIC;

public class DefaultMQHaConsumerTest {
    static final String GROUP = System.getProperty("rmqGroup", System.getenv("rmqGroup")) == null ? "group063001" : System.getProperty("rmqGroup", System.getenv("rmqGroup"));

    @Test
    public void consumerHa() throws MQClientException, IOException {
        if (DefaultMQHaProducerTest.canIgnoreRun()) {
            return;
        }
        DefaultMQPushConsumer rmqConsumer = new DefaultMQPushConsumer(
                GROUP,
                new AclClientRPCHook(new SessionCredentials(NAMESRV_LISTS1_AK, NAMESRV_LISTS1_SK)),
                new AllocateMessageQueueAveragely(),
                true,
                null
        );
        rmqConsumer.setNamesrvAddr(NAMESRV_LISTS1);

        DefaultMQHaConsumer haConsumer = DefaultMQHaConsumer.changeToHa(
                rmqConsumer,
                new LinkedHashMap<>() {
                    {
                        put("nsv01", new DefaultMQHaProducer.NameserverInfo(NAMESRV_LISTS1, NAMESRV_LISTS1_AK, NAMESRV_LISTS1_SK));
                        put("nsv02", new DefaultMQHaProducer.NameserverInfo(NAMESRV_LISTS2, NAMESRV_LISTS2_AK, NAMESRV_LISTS2_SK));
                    }
                });

        haConsumer.setMessageModel(MessageModel.CLUSTERING);
        haConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

        haConsumer.subscribe(TOPIC, "*");
        haConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                msgs.forEach(msg -> {
                    System.out.println("consuming message: " + msg);
                });
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        haConsumer.start();
        System.out.println("ha consumer start");

        System.in.read();
    }
}