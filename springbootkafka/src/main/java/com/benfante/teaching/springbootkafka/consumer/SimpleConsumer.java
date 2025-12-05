package com.benfante.teaching.springbootkafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class SimpleConsumer {
    private static final Logger log = LoggerFactory.getLogger(SimpleConsumer.class.getName());

    @KafkaListener(topics = "myTopic", groupId = "myGroup")
    public void listen(String message) {
        log.info("Received message: {}", message);
    }
}
