package com.flightstream.consumer;

import jakarta.persistence.*;

import java.time.LocalDateTime;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Telemetry class
 *
 **/
@Entity
@Table(name = "telemetry")
public class TelemetryRecord {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    @Column(name = "flight_id")
    private String flightId;

    private Double altitude;

    private Double speed;

    private Double longitude;

    private Double latitude;

    @Column(name = "timestamp")
    private LocalDateTime timestamp;


    public TelemetryRecord(){
    }

    public TelemetryRecord(String flightId, Double altitude, Double speed, Double longitude, Double latitude){
        this.flightId=flightId;
        this.altitude=altitude;
        this.longitude=longitude;
        this.latitude=latitude;
        this.speed=speed;
        this.timestamp=  LocalDateTime.now();
    }

    public String getFlightId() {
        return flightId;
    }

    public void setFlightId(String flightId) {
        this.flightId = flightId;
    }

    public Double getAltitude() {
        return altitude;
    }

    public void setAltitude(Double altitude) {
        this.altitude = altitude;
    }

    public Double getSpeed() {
        return speed;
    }

    public void setSpeed(Double speed) {
        this.speed = speed;
    }

    public Double getLongitude() {
        return longitude;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
    }

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }
}
