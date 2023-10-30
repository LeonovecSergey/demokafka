package com.epam.demokafka;

import com.epam.demokafka.dto.MessageRequest;
import com.epam.kafkademo.Message;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/kafka")
public class KafkaController {

    private final KafkaPublisher kafkaPublisher;

    public KafkaController(KafkaPublisher kafkaPublisher) {
        this.kafkaPublisher = kafkaPublisher;
    }

    @PostMapping(value = "sendMessage")
    public void sendToKafkaTopic(@RequestBody MessageRequest request) {
        var message = Message.newBuilder()
                .setId(request.getId())
                .setName(request.getName())
                .setAge(request.getAge())
                .setActive(request.isActive())
                .setEmail(request.getEmail())
                .build();

        kafkaPublisher.send(message);

        System.out.println("Id " + message.getId() + " Name " + message.getName() + " Age " + message.getAge() + " Is active " + message.getActive() + " Email " + message.getEmail());
    }
}
