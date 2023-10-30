package com.epam.demokafka;

import com.epam.kafkademo.Message;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.MessageHeaders;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {

    @KafkaListener(groupId = "my-group", topics = "message-topic", containerFactory = "avroDeserializerFactory")
    public void consumer(Message message, MessageHeaders headers) {
        System.out.println("Read the message from %S topic: %s".formatted(Constant.KAFKA_TOPIC, message));
    }
}
