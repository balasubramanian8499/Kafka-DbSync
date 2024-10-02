package com.spring.kafka_sync_consumer.service;

import com.google.gson.Gson;
import com.spring.kafka_sync_consumer.model.User;
import com.spring.kafka_sync_consumer.repo.UserRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessageListener {

    @Autowired
    UserRepo userRepository;

    Logger log = LoggerFactory.getLogger(KafkaMessageListener.class);

    @KafkaListener(topics = "testTopic1",groupId = "demo-group")
    public void consumeMessage(String message){
        log.info("consumer2 consume the message {} ", message);
    }

    @KafkaListener(groupId = "demo-group", topicPartitions = @TopicPartition(topic = "testTopic1", partitions = "0"))
    public void consumeEvents(String data) {
        Gson gson = new Gson();
        User users = new User();

        User user = gson.fromJson(data, User.class);
        users.setUserName(user.getUserName());
        users.setPassword(user.getPassword());
        users.setPhoneNumber(user.getPhoneNumber());
        userRepository.save(users);
        log.info("consumer consume the events {} ", users.toString());
    }

}
