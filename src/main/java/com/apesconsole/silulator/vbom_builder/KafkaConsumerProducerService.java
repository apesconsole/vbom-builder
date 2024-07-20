package com.apesconsole.silulator.vbom_builder;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.UUID;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class KafkaConsumerProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public KafkaConsumerProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void consumeAndSplit(String message) {
        try {
            Object jsonData = objectMapper.readValue(message, Object.class);
            List<String> chunks = splitJson(jsonData, 900000);

            String messageId = UUID.randomUUID().toString();
            int totalChunks = chunks.size();
            long start = System.currentTimeMillis();
            for (int i = 0; i < chunks.size(); i++) {
                String chunkMessage = objectMapper.writeValueAsString(new ChunkMessage(messageId, i, totalChunks, chunks.get(i)));
                kafkaTemplate.send("output_topic", messageId, chunkMessage);
            }
            long end = System.currentTimeMillis();
            log.info("Total Time taken to dump full message: " + (end - start));
            log.info("Message dropped : " + Calendar.getInstance().getTime());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<String> splitJson(Object jsonData, int maxSize) throws Exception {
        String jsonStr = objectMapper.writeValueAsString(jsonData);
        List<String> parts = new ArrayList<>();
        while (jsonStr.length() > maxSize) {
            int splitIndex = maxSize;
            while (splitIndex > 0 && jsonStr.charAt(splitIndex) != ',') {
                splitIndex--;
            }
            if (splitIndex == 0) {
                splitIndex = maxSize;
            }
            parts.add(jsonStr.substring(0, splitIndex + 1));
            jsonStr = jsonStr.substring(splitIndex + 1);
        }
        parts.add(jsonStr);
        return parts;
    }

}
