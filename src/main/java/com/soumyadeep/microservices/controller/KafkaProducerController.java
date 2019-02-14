package com.soumyadeep.microservices.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.soumyadeep.microservices.model.Car;

@RestController
public class KafkaProducerController {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerController.class);
	
	@Autowired
	private KafkaTemplate<String, Car> kafkaTemplate;
	
	  @Value("${kafka.topic.json}")
	  private String jsonTopicName;
	
	@PostMapping("/kafka/send")
	public String publish(@RequestBody Car car) {
		
		LOGGER.info("Producer sending car='{}'", car.toString());
		kafkaTemplate.send(jsonTopicName, car);
		return "Json message send successfully";
	}
	
/*	@Scheduled(fixedRate=1000)
	@GetMapping("/kafka/send")
	public String publishScheduler() {
		
		kafkaTemplate.send(jsonTopicName, new Car("Mercedes1","Benz1","Soumyadeep1"));
		LOGGER.info("Producer sending car='{}'", new Car().toString());
		return "Json message send successfully";
	}*/
}
