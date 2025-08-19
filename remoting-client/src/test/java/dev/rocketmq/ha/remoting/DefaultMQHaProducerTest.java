package dev.rocketmq.ha.remoting;

import java.util.LinkedHashMap;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.junit.Test;

public class DefaultMQHaProducerTest {
    static final String NAMESRV_LISTS1 = System.getProperty("nsv01", System.getenv("nsv01"));
    static final String NAMESRV_LISTS1_AK = System.getProperty("nsv01Ak", System.getenv("nsv01Ak"));
    static final String NAMESRV_LISTS1_SK = System.getProperty("nsv01Sk", System.getenv("nsv01Sk"));

    static final String NAMESRV_LISTS2 = System.getProperty("nsv02", System.getenv("nsv02"));
    static final String NAMESRV_LISTS2_AK = System.getProperty("nsv02Ak", System.getenv("nsv02Ak"));
    static final String NAMESRV_LISTS2_SK = System.getProperty("nsv02Sk", System.getenv("nsv02Sk"));

    static final String TOPIC = System.getProperty("rmqTopic", System.getenv("rmqTopic")) == null ? "topic063001" : System.getProperty("rmqTopic", System.getenv("rmqTopic"));

    public static boolean canIgnoreRun() {
        if (NAMESRV_LISTS1 == null || NAMESRV_LISTS1.isEmpty()) {
            System.out.println("can not run test for nameserver address not set");
            return true;
        }
        return false;
    }

    @Test
    public void sendHa() throws MQClientException {
        if (canIgnoreRun()) {
            return;
        }
        DefaultMQProducer rmqProducer = new DefaultMQProducer(
                "test",
                new AclClientRPCHook(new SessionCredentials(NAMESRV_LISTS1_AK, NAMESRV_LISTS1_SK)),
                true,
                null
        );
        rmqProducer.setNamesrvAddr(NAMESRV_LISTS1);

        DefaultMQHaProducer producer = DefaultMQHaProducer.changeToHa(
                rmqProducer,
                new LinkedHashMap<>() {
                    {
                        put("nsv01", new DefaultMQHaProducer.NameserverInfo(NAMESRV_LISTS1, NAMESRV_LISTS1_AK, NAMESRV_LISTS1_SK));
                        put("nsv02", new DefaultMQHaProducer.NameserverInfo(NAMESRV_LISTS2, NAMESRV_LISTS2_AK, NAMESRV_LISTS2_SK));
                    }
                });

        producer.start();

        for (int i = 0; i < 10; i++) {
            Message msg = new Message(TOPIC, "tag", "key", ("Hello RocketMQ " + i).getBytes());
            SendResult sr = producer.send(msg);
            System.out.println(sr);
        }
    }
}