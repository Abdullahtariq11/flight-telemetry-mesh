package com.flightstream.openskyproducer;


import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;


import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static org.springframework.http.MediaType.APPLICATION_FORM_URLENCODED;

@Service
public class OpenskyProducerService {
    private final String clientId;
    private final String clientSecret;
    private final RestTemplate restTemplate;
    private final KafkaTemplate<String,String> kafkaTemplate;


    public OpenskyProducerService(@Value("${spring.opensky.clientId}") String clientId, @Value("${spring.opensky.clientSecret}") String clientSecret, RestTemplate restTemplate, KafkaTemplate<String,String> kafkaTemplate){
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.restTemplate = restTemplate;
        this.kafkaTemplate=kafkaTemplate;
    }

    private String getToken(){
        String tokenUrl = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token";
        HttpHeaders headers= new HttpHeaders();
        headers.setContentType(APPLICATION_FORM_URLENCODED);
        MultiValueMap<String, String> body = new LinkedMultiValueMap<>();
        body.add("grant_type", "client_credentials");
        body.add("client_id", clientId);
        body.add("client_secret", clientSecret);
        HttpEntity entity=new HttpEntity<>(body,headers);
        Map response = restTemplate.postForObject(tokenUrl, entity, Map.class);
        return response.get("access_token").toString();
    }

    @Scheduled(fixedDelay = 20000)
    public void fetchAndPublish() {

        String token = getToken();
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(token);
        HttpEntity entity = new HttpEntity<>(headers);

        ResponseEntity<Map> response=restTemplate.exchange("https://opensky-network.org/api/states/all", HttpMethod.GET,entity,Map.class);
        List<List<Object>> states = (List<List<Object>>) response.getBody().get("states");
        if (states == null) return;
        for (List<Object> state : states) {

            String flightId = (String) state.get(0);
            Object lon = state.get(5);
            Object lat = state.get(6);
            Object velocity = state.get(9);
            Object geoAlt = state.get(13);
            if(lon == null || lat == null || velocity == null || geoAlt == null || Boolean.TRUE.equals(state.get(8))) {
                continue;
            }

            Double altitude= (Double) geoAlt;
            Double speed= (Double) velocity;
            Double latitude= (Double) lat;
            Double longitude=(Double) lon;
            LocalDateTime timestamp=  LocalDateTime.now();
            String template = "{\"flightId\":\"%s\",\"altitude\":%.2f,\"speed\":%.2f,\"latitude\":%.2f,\"longitude\":%.2f,\"timestamp\":\"%s\"}";
            String message=String.format(template,flightId,altitude,speed,latitude,longitude,timestamp);
            kafkaTemplate.send("flight-telemetry", flightId, message);

        }
    }
}
