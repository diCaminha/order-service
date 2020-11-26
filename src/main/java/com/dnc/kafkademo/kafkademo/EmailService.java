package com.dnc.kafkademo.kafkademo;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService {

    public static void main(String[] args) {
        var emailService = new EmailService();
        var kafkaService = new KafkaConsumerService(
                EmailService.class.getSimpleName(),
                "ECOMMERCE_SEND_EMAIL",
                emailService::parse);
        kafkaService.run();
    }

    private void parse(ConsumerRecord<String, String> record) {
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
