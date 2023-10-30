package com.epam.demokafka.configuration;

import com.epam.demokafka.Constant;
import com.epam.kafkademo.Message;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

@Configuration
public class KafkaConfiguration {

    @Bean
    public KafkaProducer<String, Message> kafkaProducer() throws IOException {

        var config = commonConfig();
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        return new KafkaProducer<>(config);
    }


    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Message> avroDeserializerFactory() throws IOException {
        ConcurrentKafkaListenerContainerFactory<String, Message> factory = new ConcurrentKafkaListenerContainerFactory<>();

        Map<String, Object> config = commonConfig().entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> entry.getKey().toString(),
                        Map.Entry::getValue
                ));
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        config.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(config));

        return factory;
    }

    private Properties commonConfig() throws IOException {
        if (!Files.exists(Paths.get(Constant.KAFKA_CONFIG_FILE))) {
            throw new IOException(Constant.KAFKA_CONFIG_FILE + " not found.");
        }
        final Properties properties = new Properties();
        try (InputStream inputStream = new FileInputStream(Constant.KAFKA_CONFIG_FILE)) {
            properties.load(inputStream);
        }
        return properties;
    }
}
