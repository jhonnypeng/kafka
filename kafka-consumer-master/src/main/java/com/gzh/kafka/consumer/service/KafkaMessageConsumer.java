package com.gzh.kafka.consumer.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;


@Component
public class KafkaMessageConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageConsumer.class);

    @KafkaListener(topics = { "${kafka.app.topic.foo}" })
    public void receive(@Payload String message, @Headers MessageHeaders headers) {
        LOG.info("KafkaMessageConsumer 接收到消息：" + message);
        headers.keySet()
                .forEach(key -> LOG.info("{}: {}", key, headers.get(key)));
    }

    @KafkaListener(id = "replyConsumer", topics = { "topic-s-r" }, containerFactory = "kafkaListenerContainerFactory")
    @SendTo
    public String returnReceive(@Payload String message, @Headers MessageHeaders headers) {
        LOG.info("KafkaMessageConsumer 接收到消息：" + message + ",并返回消息确认处理了");
        headers.keySet()
                .forEach(key -> LOG.info("{}: {}", key, headers.get(key)));
        return "return-Message";
    }
}