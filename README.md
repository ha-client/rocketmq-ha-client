# Why
在许多企业生产环境中，​​单一 RocketMQ 集群存在单点故障风险​​，无法满足金融级、跨机房、异地多活等场景下的 ​​高可用与容灾需求​​。
传统 RocketMQ 客户端只能连接 ​​单个集群​​，一旦该集群发生宕机、网络分区、Broker 宕机等故障，​​消息发送与消费将直接中断​​，影响业务连续性。

In many enterprise production environments, a single RocketMQ cluster is at risk of single - point failure and cannot meet the requirements of high availability and disaster recovery in scenarios such as financial - grade, cross - computer room, and remote multi - active.
Traditional RocketMQ clients can only connect to a single cluster. Once the cluster experiences failures such as downtime, network partition, or Broker downtime, message sending and consumption will be directly interrupted, affecting business continuity.

# Feature
| DateTime | Feature | Comment |
|-------|-------|-------|
| 2025-08-17 | support connect mutl rocketmq cluster |  |

# How to run unit test

- set 2 cluster info, topic, group

```shell

export nsv01="127.0.0.1:9876"
export nsv01Ak="your-ak"
export nsv01Sk="your-sk"

export nsv02="127.0.0.2:9876"
export nsv02Ak="your-ak"
export nsv02Sk="your-sk"

export rmqTopic="your-topic"
export rmqGroup="your-consumer-group"
```
