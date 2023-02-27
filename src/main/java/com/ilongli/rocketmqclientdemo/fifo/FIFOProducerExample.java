package com.ilongli.rocketmqclientdemo.fifo;

import com.ilongli.rocketmqclientdemo.Constants;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FIFOProducerExample {
    private static final Logger logger = LoggerFactory.getLogger(FIFOProducerExample.class);

    public static void main(String[] args) throws ClientException {
        // 消息发送的目标Topic名称，需要提前创建。
        String topic = "FIFOTopic";
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        ClientConfigurationBuilder builder = ClientConfiguration.newBuilder().setEndpoints(Constants.endpoint);
        ClientConfiguration configuration = builder.build();
        // 初始化Producer时需要设置通信配置以及预绑定的Topic。
        Producer producer = provider.newProducerBuilder()
            .setTopics(topic)
            .setClientConfiguration(configuration)
            .build();
        // FIFO消息发送。
        for (int i = 0; i < 10; i++) {
            Message message = provider.newMessageBuilder()
                    .setTopic(topic)
                    // 设置消息索引键，可根据关键字精确查找某条消息。
                    .setKeys("messageKey")
                    // 设置消息Tag，用于消费端根据指定Tag过滤消息。
                    .setTag("messageTag")
                    //设置顺序消息的排序分组，该分组尽量保持离散，避免热点排序分组。
                    .setMessageGroup("fifoGroup001")
                    // 消息体。
                    .setBody(("messageBody-" + i).getBytes())
                    .build();
            try {
                // 发送消息，需要关注发送结果，并捕获失败等异常。
                SendReceipt sendReceipt = producer.send(message);
                logger.info("Send message successfully, messageId={}", sendReceipt.getMessageId());
            } catch (ClientException e) {
                logger.error("Failed to send message", e);
            }
        }
        // producer.close();
    }
}