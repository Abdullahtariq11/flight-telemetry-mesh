package com.flightstream.consumer;

import org.springframework.stereotype.Service;

import java.util.HashMap;

@Service
public class SafetyAlertEngine {
    HashMap<String,TelemetryRecord> flightReadings;

    public SafetyAlertEngine(){
        flightReadings= new HashMap<>();
    }

    public void evaluate(TelemetryRecord telemetryRecord){
        if(telemetryRecord == null){
            throw new IllegalArgumentException("Telemetry Record empty.");
        }
        TelemetryRecord previousRecord= flightReadings.get(telemetryRecord.getFlightId());
        if(previousRecord == null){
            flightReadings.put(telemetryRecord.getFlightId(),telemetryRecord);
            return;
        }
        double drop = previousRecord.getAltitude() - telemetryRecord.getAltitude();
        long seconds = java.time.Duration.between(previousRecord.getTimestamp(), telemetryRecord.getTimestamp()).getSeconds();

        if(drop>=2000 && seconds <= 10){
            System.out.println("🚨 RAPID DESCENT ALERT — Flight " + telemetryRecord.getFlightId() + " dropped " + drop + " feet in " + seconds + " seconds");
        }
        flightReadings.put(telemetryRecord.getFlightId(),telemetryRecord);
    }


}
