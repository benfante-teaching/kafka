package com.benfante.teaching.kafka.clients;

import java.time.Duration;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelloAdmin {

    private static final Logger log = LoggerFactory.getLogger(HelloAdmin.class);

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:19092,localhost:29092,localhost:39092");
        Admin admin = null;
        try {
            admin = Admin.create(props);
            listTopics(admin);
            // createTopic(admin, "course.admin.test", 3, (short) 3);
            // describeTopic(admin, "course.admin.test");
            // deleteTopic(admin, "course.admin.test");
        } finally {
            if (admin != null) {
                admin.close(Duration.ofSeconds(30));
            }
        }
    }

    public static void listTopics(Admin admin) throws InterruptedException, ExecutionException {
        ListTopicsResult topics = admin.listTopics();
        Set<String> names = topics.names().get();
        for (String name : names) {
            System.out.println("Topic: " + name);
        }
    }

    public static void createTopic(Admin admin, String topicName, int numPartitions,
            short replicationFactor) throws InterruptedException, ExecutionException {
        CreateTopicsResult newTopic = admin.createTopics(Collections
                .singletonList(new NewTopic(topicName, numPartitions, replicationFactor)));
        for (Entry<String, KafkaFuture<Void>> topic : newTopic.values().entrySet()) {
            try {
                topic.getValue().get();
                log.info("Topic {} created successfully", topic.getKey());
            } catch (Exception e) {
                log.error("Error creating topic {}", topic.getKey(), e);
            }
        }
    }

    public static void describeTopic(Admin admin, String topicName)
            throws InterruptedException, ExecutionException {
        DescribeTopicsResult demoTopic = admin.describeTopics(Collections.singletonList(topicName));
        TopicDescription topicDescription = demoTopic.allTopicNames().get().get(topicName);
        log.info("Description of demo topic: {}", topicDescription);
    }

    public static void deleteTopic(Admin admin, String topicName)
            throws InterruptedException, ExecutionException {
        admin.deleteTopics(Collections.singletonList(topicName)).all().get();
        log.info("Topic {} deleted successfully", topicName);
    }
}
