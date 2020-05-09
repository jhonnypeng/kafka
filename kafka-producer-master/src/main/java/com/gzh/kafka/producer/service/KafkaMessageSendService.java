package com.gzh.kafka.producer.service;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

@Service
public class KafkaMessageSendService {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageSendService.class);

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    @Autowired
    private ReplyingKafkaTemplate<String,String,String> replyingKafkaTemplate;

    @Value("${kafka.app.topic.foo}")
    private String topic;

    public void send(String message) {
        LOG.info("topic=" + topic + ",message=" + message);
        ListenableFuture<SendResult<String,String>> future = kafkaTemplate.send(topic, message);
        future.addCallback(success -> LOG.info("KafkaMessageProducer 发送消息成功！"),
                fail -> LOG.error("KafkaMessageProducer 发送消息失败！"));
    }


    public String sendReceive(String message) throws InterruptedException, ExecutionException {
        LOG.info("topic=topic-s-r,message=" + message);
        ProducerRecord<String,String> producerRecord = new ProducerRecord<String,String>("topic-s-r", message);
        producerRecord.headers()
                .add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, "topic.quick.reply".getBytes()));
        RequestReplyFuture<String,String,String> future = replyingKafkaTemplate.sendAndReceive(producerRecord);
        future.addCallback(success -> LOG.info("KafkaMessageProducer 发送消息成功！"),
                fail -> LOG.error("KafkaMessageProducer 发送消息失败！"));

        ConsumerRecord<String,String> consumerRecord = future.get();
        System.out.println("===============>Return value: " + consumerRecord.toString());
        return consumerRecord.value();
    }

}