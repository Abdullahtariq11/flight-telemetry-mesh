package com.flightstream.producer;


import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Random;


@Component
public class TelemetryProducer {
    private final KafkaTemplate<String,String> kafkaTemplate;

    public TelemetryProducer(KafkaTemplate kafkaTemplate){
        this.kafkaTemplate=kafkaTemplate;

    }

    public void simulateFlight(String flightId){
        Random r= new Random();
        while(true){
            Double altitude= r.nextDouble(40000-27000)+27000;
            Double speed= r.nextDouble(700-400)+400;
            Double latitude= r.nextDouble(60-40)+40;
            Double longitude=r.nextDouble(60)-130;
            LocalDateTime timestamp=  LocalDateTime.now();
            String template = "{\"flightId\":\"%s\",\"altitude\":%.2f,\"speed\":%.2f,\"latitude\":%.2f,\"longitude\":%.2f,\"timestamp\":\"%s\"}";
            String message=String.format(template,flightId,altitude,speed,latitude,longitude,timestamp);
            kafkaTemplate.send("flight-telemetry", flightId, message);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void startSimulation(int numFlights) {
        for (int i = 0; i < numFlights; i++) {
            String flightId = String.format("FL%04d", i);
            Thread.ofVirtual().start(() -> simulateFlight(flightId));
        }
    }

}
