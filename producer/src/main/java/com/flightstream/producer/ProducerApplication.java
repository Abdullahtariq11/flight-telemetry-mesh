package com.flightstream.producer;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ProducerApplication implements CommandLineRunner {
    private final TelemetryProducer telemetryProducer;

    public ProducerApplication(TelemetryProducer telemetryProducer){
        this.telemetryProducer=telemetryProducer;
    }

    public static void main(String[] args) {
        SpringApplication.run(ProducerApplication.class, args);
    }

    @Override
    public void run(String ...args) throws InterruptedException {
        telemetryProducer.startSimulation(1000);
        Thread.currentThread().join();
    }

}
