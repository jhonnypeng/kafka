package com.gzh.kafka.producer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;

@SpringBootApplication
@EnableConfigurationProperties
@EnableKafka
public class KafkaProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaProducerApplication.class, args);
    }

    // 配件：监听器容器Listener Container to be set up in ReplyingKafkaTemplate
    @Bean
    public KafkaMessageListenerContainer<String,String> replyContainer() {
        // 设置的接收topic必须和设置的RecordHeader里的topic一致topic.quick.reply
        ContainerProperties containerProperties = new ContainerProperties("topic.quick.reply");
        return new KafkaMessageListenerContainer<>(new DefaultKafkaConsumerFactory<>(consumerConfigs()),
                containerProperties);
    }

    // 这是核心的ReplyingKafkaTemplate
    @Bean
    public ReplyingKafkaTemplate<String,String,String> replyingKafkaTemplate(
            ProducerFactory<String,String> producerFactory,
            KafkaMessageListenerContainer<String,String> repliesContainer) {
        ReplyingKafkaTemplate<String,String,String> replyingKafkaTemplate = new ReplyingKafkaTemplate<>(producerFactory,
                repliesContainer);
        // 响应时间设置
        replyingKafkaTemplate.setDefaultReplyTimeout(Duration.ofMinutes(5));
        return replyingKafkaTemplate;
    }

    @Bean
    public Map<String,Object> consumerConfigs() {
        Map<String,Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.203.129:9092");
        // 默认的group_id
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "repliesGroup");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }

}