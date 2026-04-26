package com.flightstream.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

@Service
public class TelemetryConsumer {
    private final TelemetryRepository telemetryRepository;
    private final ObjectMapper objectMapper= new ObjectMapper();



    public TelemetryConsumer(TelemetryRepository telemetryRepository){
        this.telemetryRepository=telemetryRepository;
        objectMapper.registerModule(new JavaTimeModule());

    }

    @KafkaListener(topics = "flight-telemetry", groupId = "flightstream-group")
    public void printMessage(String message) throws JsonProcessingException {
        TelemetryRecord record=objectMapper.readValue(message, TelemetryRecord.class);
        telemetryRepository.save(record);
        System.out.println(record.getFlightId());
    }

}
