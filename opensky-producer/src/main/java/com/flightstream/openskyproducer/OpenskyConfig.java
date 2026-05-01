package com.flightstream.openskyproducer;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

@Configuration
public class OpenskyConfig {

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }
}