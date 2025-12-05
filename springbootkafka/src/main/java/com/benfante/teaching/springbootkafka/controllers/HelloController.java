package com.benfante.teaching.springbootkafka.controllers;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;


@RestController
@RequestMapping("/")
public class HelloController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @GetMapping
    public String sayHello() {
        return "Hello, World!";
    }

    @GetMapping("/send")
    public String sendMessage(@RequestParam(defaultValue = "Hello from Spring Boot Kafka!") String message) {
        kafkaTemplate.send("myTopic", message);
        return "Message sent to Kafka topic!";
    }
}
