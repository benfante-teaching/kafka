package com.benfante.teaching.kafka.clients;

import java.time.LocalDateTime;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelloProducer {

    private static final Logger log = LoggerFactory.getLogger(HelloProducer.class);

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        sendASync("course_helloworld", "anotherkey1", "Hello, Kafka! " + LocalDateTime.now());
    }

    public static void send(String topic, String key, String value) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:19092,localhost:29092,localhost:39092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, key, value);            
            producer.send(record);
            producer.flush();
        }
    }

    public static void sendSync(String topic, String key, String value) throws InterruptedException, ExecutionException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:19092,localhost:29092,localhost:39092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, key, value);
            RecordMetadata recordMetadata = producer.send(record).get();
            log.info("Record sent to partition {} with offset {}",
                    recordMetadata.partition(), recordMetadata.offset());
            producer.flush();
        }
    }

    public static void sendASync(String topic, String key, String value) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:19092,localhost:29092,localhost:39092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, key, value);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Error sending record", exception);
                } else {
                    log.info("Record sent to partition {} with offset {}",
                            metadata.partition(), metadata.offset());
                }
            });
            producer.flush();
        }
    }

}
