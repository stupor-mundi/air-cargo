package io.aircargo.analyser.statemachine;

import io.aircargo.analyser.proto.FlightPhase;

import java.io.Serializable;

/**
 * Per-aircraft mutable state held in Flink RocksDB ValueState.
 *
 * <p>One instance per ICAO24 key. Must implement {@link Serializable}
 * so Flink can serialise it via Kryo into the state backend.
 *
 * <p>{@link #reset()} clears flight-specific fields on landing but
 * preserves last-known position and anomaly score across flights.
 */
public class FlightState implements Serializable {

    private static final long serialVersionUID = 1L;

    // ---- Phase -------------------------------------------------------

    private FlightPhase phase = FlightPhase.FLIGHT_PHASE_UNKNOWN;

    // ---- Flight identity ---------------------------------------------

    /** System-generated: {@code icao24 + "_" + takeoffMs}. Null until takeoff detected. */
    private String flightId;

    /** Latest callsign seen for this aircraft. Nullable. */
    private String currentCallsign;

    // ---- Last known position -----------------------------------------

    private Double lastLat;
    private Double lastLon;
    private Double lastAltitudeM;
    private Double lastVelocityMs;
    private Double lastVerticalRateMs;
    private Double lastHeadingDeg;

    /** Milliseconds — from AircraftPosition.snapshotTimeMs. */
    private Long lastSeenMs;

    // ---- Current flight context --------------------------------------

    /** When takeoff was detected (ms). Null if unknown. */
    private Long takeoffMs;
    private Double takeoffLat;
    private Double takeoffLon;

    /** Airport ICAO ident where this flight originated. Null if unknown or on ground. */
    private String departedFrom;

    // ---- Gap tracking ------------------------------------------------

    /** When tracking was lost (ms). Null if not currently lost. */
    private Long lostTrackingMs;

    // ---- Anomaly scoring ---------------------------------------------

    /** Running anomaly score 0.0–1.0. Preserved across flights. */
    private double anomalyScore = 0.0;

    // ------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------

    public FlightState() {
    }

    // ------------------------------------------------------------------
    // reset() — called on landing; clears flight fields, keeps last-seen
    // ------------------------------------------------------------------

    /**
     * Clears flight-specific fields after landing or when tracking is closed.
     * Preserves last-known position fields and {@link #anomalyScore}.
     */
    public void reset() {
        flightId = null;
        takeoffMs = null;
        takeoffLat = null;
        takeoffLon = null;
        departedFrom = null;
    }

    // ------------------------------------------------------------------
    // Getters and setters
    // ------------------------------------------------------------------

    public FlightPhase getPhase() { return phase; }
    public void setPhase(FlightPhase phase) { this.phase = phase; }

    public String getFlightId() { return flightId; }
    public void setFlightId(String flightId) { this.flightId = flightId; }

    public String getCurrentCallsign() { return currentCallsign; }
    public void setCurrentCallsign(String currentCallsign) { this.currentCallsign = currentCallsign; }

    public Double getLastLat() { return lastLat; }
    public void setLastLat(Double lastLat) { this.lastLat = lastLat; }

    public Double getLastLon() { return lastLon; }
    public void setLastLon(Double lastLon) { this.lastLon = lastLon; }

    public Double getLastAltitudeM() { return lastAltitudeM; }
    public void setLastAltitudeM(Double lastAltitudeM) { this.lastAltitudeM = lastAltitudeM; }

    public Double getLastVelocityMs() { return lastVelocityMs; }
    public void setLastVelocityMs(Double lastVelocityMs) { this.lastVelocityMs = lastVelocityMs; }

    public Double getLastVerticalRateMs() { return lastVerticalRateMs; }
    public void setLastVerticalRateMs(Double lastVerticalRateMs) { this.lastVerticalRateMs = lastVerticalRateMs; }

    public Double getLastHeadingDeg() { return lastHeadingDeg; }
    public void setLastHeadingDeg(Double lastHeadingDeg) { this.lastHeadingDeg = lastHeadingDeg; }

    public Long getLastSeenMs() { return lastSeenMs; }
    public void setLastSeenMs(Long lastSeenMs) { this.lastSeenMs = lastSeenMs; }

    public Long getTakeoffMs() { return takeoffMs; }
    public void setTakeoffMs(Long takeoffMs) { this.takeoffMs = takeoffMs; }

    public Double getTakeoffLat() { return takeoffLat; }
    public void setTakeoffLat(Double takeoffLat) { this.takeoffLat = takeoffLat; }

    public Double getTakeoffLon() { return takeoffLon; }
    public void setTakeoffLon(Double takeoffLon) { this.takeoffLon = takeoffLon; }

    public Long getLostTrackingMs() { return lostTrackingMs; }
    public void setLostTrackingMs(Long lostTrackingMs) { this.lostTrackingMs = lostTrackingMs; }

    public String getDepartedFrom() { return departedFrom; }
    public void setDepartedFrom(String departedFrom) { this.departedFrom = departedFrom; }

    public double getAnomalyScore() { return anomalyScore; }
    public void setAnomalyScore(double anomalyScore) { this.anomalyScore = anomalyScore; }
}
