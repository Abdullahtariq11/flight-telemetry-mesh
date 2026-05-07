package com.flightstream.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;

@Service
public class TelemetryConsumer {
    private final TelemetryRepository telemetryRepository;
    private final ObjectMapper objectMapper= new ObjectMapper();
    private final SafetyAlertEngine safetyAlertEngine;
    private final HeartbeatService heartbeatService;



    public TelemetryConsumer(TelemetryRepository telemetryRepository, SafetyAlertEngine safetyAlertEngine,
    HeartbeatService heartbeatService){
        this.telemetryRepository=telemetryRepository;
        objectMapper.registerModule(new JavaTimeModule());
        this.safetyAlertEngine=safetyAlertEngine;
        this.heartbeatService=heartbeatService;

    }

    @KafkaListener(topics = "flight-telemetry", groupId = "flightstream-group")
    public void printMessage(String message) throws IOException {
        TelemetryRecord record=objectMapper.readValue(message, TelemetryRecord.class);
        telemetryRepository.save(record);
        safetyAlertEngine.evaluate(record);
        heartbeatService.recordHeartbeat(record.getFlightId());
        System.out.println(record.getFlightId());
    }

}
