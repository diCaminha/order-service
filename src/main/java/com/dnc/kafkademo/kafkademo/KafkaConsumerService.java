package com.dnc.kafkademo.kafkademo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerService {

    private String groupId;
    private final ConsumerFunction parse;
    private final KafkaConsumer<String, String> consumer;

    public KafkaConsumerService(String groupId, String topic, ConsumerFunction parse) {
        this.groupId = groupId;
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(properties());
        this.consumer.subscribe(Collections.singletonList(topic));
    }

    public void run() {
        while (true) {
            var records = this.consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                System.out.println("Encontrei registros: " + records.count());
                records.forEach(parse::consume);
            }
        }
    }
    private Properties properties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);

        return properties;
    }
}
