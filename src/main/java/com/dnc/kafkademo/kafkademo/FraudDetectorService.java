package com.dnc.kafkademo.kafkademo;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudDetectorService {

    public static void main(String[] args) {
        var fraudDetectorService = new FraudDetectorService();
        var kafkaService = new KafkaConsumerService(
                FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudDetectorService::parse);
        kafkaService.run();
    }

    public void parse(ConsumerRecord<String, String> record) {
        System.out.println("Processing new order. Checking for fraud...");
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
