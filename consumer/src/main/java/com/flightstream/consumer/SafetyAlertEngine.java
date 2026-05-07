package com.flightstream.consumer;

import com.flightstream.consumer.handler.SocketConnectionHandler;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;

@Service
public class SafetyAlertEngine {
    HashMap<String,TelemetryRecord> flightReadings;
    final private SocketConnectionHandler socketConnectionHandler;

    public SafetyAlertEngine(SocketConnectionHandler socketConnectionHandler){
        this.socketConnectionHandler= socketConnectionHandler;
        flightReadings= new HashMap<>();
    }

    public void evaluate(TelemetryRecord telemetryRecord) throws IOException {
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
            String message="🚨 RAPID DESCENT ALERT,  Flight " + telemetryRecord.getFlightId() + " dropped " + drop + " feet in " + seconds + " seconds";
            socketConnectionHandler.sendAlert(message);
            System.out.println(message);
        }
        flightReadings.put(telemetryRecord.getFlightId(),telemetryRecord);
    }


}
