package com.flightstream.openskyproducer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class OpenskyProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(OpenskyProducerApplication.class, args);
    }

}
