package com.soumyadeep.microservices.controller;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import com.soumyadeep.microservices.model.Car;

@Service
public class Receiver {

	private static final Logger LOGGER = LoggerFactory.getLogger(Receiver.class);

	@Value("${kafka.topic.json}")
	private String jsonTopic;

	/*
	 * For testing convenience, we added a CountDownLatch. This allows the POJO to
	 * signal that a message is received. This is something we will
	 * implement in a production application.
	 */
	CountDownLatch latch = new CountDownLatch(1);

	public CountDownLatch getLatch() {
		return latch;
	}

	// Normal Kafka Listening $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
/*	@KafkaListener(topics = "${kafka.topic.json}", groupId = "json")
	public void consumeNormally(Car car) {
		LOGGER.info("Consumer receiving car='{}'", car.toString());
		latch.countDown();
	}*/
	
	//One consumer can listen for messages from various topics @KafkaListener(topics = "topic1, topic2", groupId = "foo")$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
	 

	// Spring also supports retrieval of one or more message headers using the @Header annotation in the listener $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
/*	@KafkaListener(topics = "jsonTopic")
	public void consumeWithAnyPartition(Car car, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
		LOGGER.info("Consumer receiving car='{}'", car.toString());
		LOGGER.info("Received Message:" + car.toString() + "from partition: " + partition);
	}*/

	
	
	// For a topic with multiple partitions, a @KafkaListener can explicitly subscribe to a particular partition of a topic with an initial offset $$$$$$$$$$$$$$$$$$$$$$$$$$
	@KafkaListener(topicPartitions = @TopicPartition(topic = "jsonTopic", partitionOffsets = {
			@PartitionOffset(partition = "0", initialOffset = "0"),
			@PartitionOffset(partition = "2", initialOffset = "0") }))
	
/*	@KafkaListener(topicPartitions 
			  = @TopicPartition(topic = "jsonTopic", partitions = { "0", "2" }))*/
	public void consumeWithSpecificPartition(Car car, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
		LOGGER.info("Consumer receiving car='{}'", car.toString());
		LOGGER.info("Received Message:" + car.toString() + "from partition: " + partition);
	}
}
