package com.flightstream.consumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.concurrent.TimeUnit;

@Service
public class HeartbeatService {

    private StringRedisTemplate redisTemplate;

    public HeartbeatService(StringRedisTemplate redisTemplate){
        this.redisTemplate=redisTemplate;
    }

    public void recordHeartbeat(String flightId){
        redisTemplate.opsForValue().set(
                "flight:" + flightId + ":last_seen",
                "active",
                35,
                TimeUnit.SECONDS
        );
    }

    @Scheduled(fixedDelay = 10000)
    public void checkSilentFlights(){
        Set<String> keys = redisTemplate.keys("flight:*:last_seen");
        if(keys == null || keys.isEmpty()){
            System.out.println("No active flights detected");
            return;
        }
        for(String key : keys){
            String[] parts= key.split(":");
            String flightId=parts[1];
            System.out.println(flightId + " is active");
        }
    }


}
