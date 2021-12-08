package com.varobank.kafka.neptune.utils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.stream.Collectors;

@Configuration
public class TopicUtil {

    @Autowired
    private ApplicationContext applicationContext;

    @Value("#{'${app.kafka.consumer.topics}'.split(',')}")
    private List<String> consumerTopics;

    @Bean
    public List<String> topics() {
        return consumerTopics.stream().map(p -> p.trim()).collect(Collectors.toList());
    }

    @Bean
    public List<String> retryTopics() {
        return consumerTopics.stream().map(p -> p.trim() + "__" + applicationContext.getId() + "__retry").collect(Collectors.toList());
    }
}
