package com.epam.demokafka;

import com.epam.kafkademo.Message;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

@Component
public class KafkaPublisher {
    private final KafkaProducer<String, Message> kafkaProducer;

    public KafkaPublisher(KafkaProducer<String, Message> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    public void send(Message message) {
        kafkaProducer.send(new ProducerRecord<>(Constant.KAFKA_TOPIC, message), (metadata, e) -> {
            if (e == null) {
                System.out.println("Success!");
                System.out.println(metadata.toString());
            } else {
                e.printStackTrace();
            }
        });
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
