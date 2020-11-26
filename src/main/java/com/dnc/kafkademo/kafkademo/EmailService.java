package com.dnc.kafkademo.kafkademo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class EmailService {

    public static void main(String[] args) {
        var emailService = new EmailService();
        var kafkaService = new KafkaConsumerService("ECOMMERCE_SEND_EMAIL", EmailService::parse);
        kafkaService.run();
    }

    private static void parse(ConsumerRecord<String, String> record) {
        System.out.println("Sending Email about order...");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Order processed.");
    }
}
