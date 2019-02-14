package com.soumyadeep.microservices.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
public class KafkaTopicConfig {

	@Value("${kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Value("${kafka.topic.json}")
	private String jsonTopicName;

	@Bean
	public KafkaAdmin kafkaAdmin() {
		Map<String, Object> configs = new HashMap<>();
		configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		return new KafkaAdmin(configs);
	}

	/*
	 * Kafka broker stores all messages in the partitions configured for that
	 * particular topic. It ensures the messages are equally shared between
	 * partitions. If the producer sends two messages and there are two partitions,
	 * Kafka will store one message in the first partition and the second message in
	 * the second partition.
	 */
	@Bean
	public NewTopic createTopic() {
		return new NewTopic(jsonTopicName, 3, (short) 3);
	}
}
