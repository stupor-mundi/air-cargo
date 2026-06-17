package io.aircargo.common.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Canonical per-aircraft position observation, produced by the snapshot-explode job
 * (adsb-analyser {@code SnapshotSplitter}) for each state vector in an OpenSky
 * {@code /states/all} snapshot.
 *
 * <p>Field names correspond to the OpenSky state vector indices documented in the
 * project rules. Index 12 (sensors) is deliberately omitted.
 *
 * <p><strong>snapshotTimeMs</strong> is the poll timestamp — the moment the
 * {@code /states/all} request was made — converted to milliseconds. It is
 * <em>not</em> the time of the individual position fix; use {@link #timePosition}
 * (unix seconds) for that.
 *
 * <p><strong>onGround</strong> is unreliable and should be treated as a hint only.
 * Use {@link #isAirborne()} rather than reading the field directly.
 *
 * <p>The following fields are intentionally absent from this class. They are
 * assigned by downstream Flink operators and are not part of the raw observation:
 * {@code flightId}, {@code flightPhase}, {@code anomalyScore}, {@code aircraftType},
 * {@code operator}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AircraftPosition {

    /** ICAO 24-bit transponder address (hex string). Index 0. Never null. */
    private String icao24;

    /** Flight callsign, up to 8 characters, whitespace pre-trimmed. Index 1. Nullable. */
    private String callsign;

    /** Country inferred from ICAO24 prefix. Index 2. Nullable. */
    private String originCountry;

    /** Unix timestamp (seconds) of the last position fix. Index 3. Nullable. */
    private Long timePosition;

    /** Unix timestamp (seconds) of the last ADS-B contact. Index 4. Nullable. */
    private Long lastContact;

    /** WGS-84 longitude in decimal degrees. Index 5. Nullable. */
    private Double longitude;

    /** WGS-84 latitude in decimal degrees. Index 6. Nullable. */
    private Double latitude;

    /** Barometric altitude in metres. Index 7. Nullable. */
    private Double baroAltitudeM;

    /**
     * Whether the aircraft is on the ground according to the transponder.
     * Index 8. Nullable. Unreliable — treat as hint only; prefer {@link #isAirborne()}.
     */
    private Boolean onGround;

    /** Ground speed in metres per second. Index 9. Nullable. */
    private Double velocityMs;

    /** True track (heading) in degrees clockwise from north. Index 10. Nullable. */
    private Double headingDeg;

    /** Vertical rate in metres per second; positive = climbing. Index 11. Nullable. */
    private Double verticalRateMs;

    /** Geometric (GPS) altitude in metres. Index 13. Nullable. */
    private Double geoAltitudeM;

    /** Transponder squawk code. Index 14. Nullable. */
    private String squawk;

    /** Special purpose indicator. Index 15. Nullable. */
    private Boolean spi;

    /**
     * Position source: 0=ADS-B, 1=ASTERIX, 2=MLAT, 3=FLARM.
     * Index 16. Nullable.
     */
    private Integer positionSource;

    /**
     * Aircraft wake turbulence category (extended=1 only).
     * 0=no info, 4=large (75k–300k lbs), 5=high vortex large (B-757),
     * 6=heavy (&gt;300k lbs). Index 17. Nullable.
     */
    private Integer category;

    /**
     * Snapshot poll timestamp in milliseconds ({@code "time"} field from the
     * OpenSky response × 1000). This is when the collector queried the API,
     * not the time of the position fix.
     */
    private Long snapshotTimeMs;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    public AircraftPosition() {
    }

    public AircraftPosition(
            String icao24,
            String callsign,
            String originCountry,
            Long timePosition,
            Long lastContact,
            Double longitude,
            Double latitude,
            Double baroAltitudeM,
            Boolean onGround,
            Double velocityMs,
            Double headingDeg,
            Double verticalRateMs,
            Double geoAltitudeM,
            String squawk,
            Boolean spi,
            Integer positionSource,
            Integer category,
            Long snapshotTimeMs) {
        this.icao24 = icao24;
        this.callsign = callsign;
        this.originCountry = originCountry;
        this.timePosition = timePosition;
        this.lastContact = lastContact;
        this.longitude = longitude;
        this.latitude = latitude;
        this.baroAltitudeM = baroAltitudeM;
        this.onGround = onGround;
        this.velocityMs = velocityMs;
        this.headingDeg = headingDeg;
        this.verticalRateMs = verticalRateMs;
        this.geoAltitudeM = geoAltitudeM;
        this.squawk = squawk;
        this.spi = spi;
        this.positionSource = positionSource;
        this.category = category;
        this.snapshotTimeMs = snapshotTimeMs;
    }

    // -------------------------------------------------------------------------
    // Convenience methods
    // -------------------------------------------------------------------------

    /**
     * Returns {@code true} if both {@link #latitude} and {@link #longitude} are
     * non-null. OpenSky omits position fields for aircraft that are not broadcasting
     * a valid GPS fix.
     */
    public boolean hasPosition() {
        return latitude != null && longitude != null;
    }

    /**
     * Returns {@code true} only when {@link #onGround} is explicitly
     * {@link Boolean#FALSE}. Returns {@code false} for {@code null} (unknown) and
     * for {@code true} (confirmed on ground). Do not use {@code onGround} directly —
     * the field is unreliable.
     */
    public boolean isAirborne() {
        return Boolean.FALSE.equals(onGround);
    }

    // -------------------------------------------------------------------------
    // Builder
    // -------------------------------------------------------------------------

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private String icao24;
        private String callsign;
        private String originCountry;
        private Long timePosition;
        private Long lastContact;
        private Double longitude;
        private Double latitude;
        private Double baroAltitudeM;
        private Boolean onGround;
        private Double velocityMs;
        private Double headingDeg;
        private Double verticalRateMs;
        private Double geoAltitudeM;
        private String squawk;
        private Boolean spi;
        private Integer positionSource;
        private Integer category;
        private Long snapshotTimeMs;

        private Builder() {
        }

        public Builder icao24(String icao24) { this.icao24 = icao24; return this; }
        public Builder callsign(String callsign) { this.callsign = callsign; return this; }
        public Builder originCountry(String originCountry) { this.originCountry = originCountry; return this; }
        public Builder timePosition(Long timePosition) { this.timePosition = timePosition; return this; }
        public Builder lastContact(Long lastContact) { this.lastContact = lastContact; return this; }
        public Builder longitude(Double longitude) { this.longitude = longitude; return this; }
        public Builder latitude(Double latitude) { this.latitude = latitude; return this; }
        public Builder baroAltitudeM(Double baroAltitudeM) { this.baroAltitudeM = baroAltitudeM; return this; }
        public Builder onGround(Boolean onGround) { this.onGround = onGround; return this; }
        public Builder velocityMs(Double velocityMs) { this.velocityMs = velocityMs; return this; }
        public Builder headingDeg(Double headingDeg) { this.headingDeg = headingDeg; return this; }
        public Builder verticalRateMs(Double verticalRateMs) { this.verticalRateMs = verticalRateMs; return this; }
        public Builder geoAltitudeM(Double geoAltitudeM) { this.geoAltitudeM = geoAltitudeM; return this; }
        public Builder squawk(String squawk) { this.squawk = squawk; return this; }
        public Builder spi(Boolean spi) { this.spi = spi; return this; }
        public Builder positionSource(Integer positionSource) { this.positionSource = positionSource; return this; }
        public Builder category(Integer category) { this.category = category; return this; }
        public Builder snapshotTimeMs(Long snapshotTimeMs) { this.snapshotTimeMs = snapshotTimeMs; return this; }

        public AircraftPosition build() {
            return new AircraftPosition(
                    icao24, callsign, originCountry, timePosition, lastContact,
                    longitude, latitude, baroAltitudeM, onGround, velocityMs,
                    headingDeg, verticalRateMs, geoAltitudeM, squawk, spi,
                    positionSource, category, snapshotTimeMs);
        }
    }

    // -------------------------------------------------------------------------
    // Getters and setters
    // -------------------------------------------------------------------------

    public String getIcao24() { return icao24; }
    public void setIcao24(String icao24) { this.icao24 = icao24; }

    public String getCallsign() { return callsign; }
    public void setCallsign(String callsign) { this.callsign = callsign; }

    public String getOriginCountry() { return originCountry; }
    public void setOriginCountry(String originCountry) { this.originCountry = originCountry; }

    public Long getTimePosition() { return timePosition; }
    public void setTimePosition(Long timePosition) { this.timePosition = timePosition; }

    public Long getLastContact() { return lastContact; }
    public void setLastContact(Long lastContact) { this.lastContact = lastContact; }

    public Double getLongitude() { return longitude; }
    public void setLongitude(Double longitude) { this.longitude = longitude; }

    public Double getLatitude() { return latitude; }
    public void setLatitude(Double latitude) { this.latitude = latitude; }

    public Double getBaroAltitudeM() { return baroAltitudeM; }
    public void setBaroAltitudeM(Double baroAltitudeM) { this.baroAltitudeM = baroAltitudeM; }

    public Boolean getOnGround() { return onGround; }
    public void setOnGround(Boolean onGround) { this.onGround = onGround; }

    public Double getVelocityMs() { return velocityMs; }
    public void setVelocityMs(Double velocityMs) { this.velocityMs = velocityMs; }

    public Double getHeadingDeg() { return headingDeg; }
    public void setHeadingDeg(Double headingDeg) { this.headingDeg = headingDeg; }

    public Double getVerticalRateMs() { return verticalRateMs; }
    public void setVerticalRateMs(Double verticalRateMs) { this.verticalRateMs = verticalRateMs; }

    public Double getGeoAltitudeM() { return geoAltitudeM; }
    public void setGeoAltitudeM(Double geoAltitudeM) { this.geoAltitudeM = geoAltitudeM; }

    public String getSquawk() { return squawk; }
    public void setSquawk(String squawk) { this.squawk = squawk; }

    public Boolean getSpi() { return spi; }
    public void setSpi(Boolean spi) { this.spi = spi; }

    public Integer getPositionSource() { return positionSource; }
    public void setPositionSource(Integer positionSource) { this.positionSource = positionSource; }

    public Integer getCategory() { return category; }
    public void setCategory(Integer category) { this.category = category; }

    public Long getSnapshotTimeMs() { return snapshotTimeMs; }
    public void setSnapshotTimeMs(Long snapshotTimeMs) { this.snapshotTimeMs = snapshotTimeMs; }
}
