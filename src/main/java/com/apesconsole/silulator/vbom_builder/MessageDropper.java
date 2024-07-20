package com.apesconsole.silulator.vbom_builder;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
public class MessageDropper {
	
	@Autowired
	private JsonGenerator jsonGenerator;
	
	@Autowired
	private KafkaConsumerProducerService kafkaConsumerProducerService;

	@GetMapping("/drop")
	public void drop() throws IOException {
		log.info("Dropping 1 large message");
		kafkaConsumerProducerService.consumeAndSplit(jsonGenerator.readFile("large.json"));
	}
}
