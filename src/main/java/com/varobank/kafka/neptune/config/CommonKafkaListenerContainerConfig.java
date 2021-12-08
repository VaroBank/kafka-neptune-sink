package com.varobank.kafka.neptune.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.BatchLoggingErrorHandler;
import org.springframework.kafka.listener.LoggingErrorHandler;

import java.util.Map;

@Configuration
public class CommonKafkaListenerContainerConfig {

    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServers;

    @Autowired
    private KafkaProperties kafkaProperties;

    private DefaultKafkaConsumerFactory consumerFactory;
    private DefaultKafkaProducerFactory producerFactory;

    public synchronized ConsumerFactory<String, String> consumerFactory() {
        if (consumerFactory != null) {
            return consumerFactory;
        } else {
            Map<String, Object> props = kafkaProperties.buildConsumerProperties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

            consumerFactory = new DefaultKafkaConsumerFactory<>(props);

            return consumerFactory;
        }
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaBatchListenerContainerFactory(){
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());
        factory.setBatchListener(true);
        factory.setConcurrency(kafkaProperties.getListener().getConcurrency());
        factory.getContainerProperties().setAckMode(kafkaProperties.getListener().getAckMode());
        factory.setBatchErrorHandler(new BatchLoggingErrorHandler());
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(){
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());
        factory.setBatchListener(false);
        factory.getContainerProperties().setAckMode(kafkaProperties.getListener().getAckMode());
        factory.setErrorHandler(new LoggingErrorHandler());
        return factory;
    }

    public synchronized ProducerFactory<String, String> producerFactory() {
        if (producerFactory != null) {
            return producerFactory;
        } else {
            Map<String, Object> props = kafkaProperties.buildProducerProperties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

            producerFactory = new DefaultKafkaProducerFactory<>(props);

            return producerFactory;
        }
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
