package io.aircargo.analyser.model;

/**
 * A single aircraft position record exploded from an OpenSky /states/all snapshot.
 *
 * Nullable fields use boxed types. snapshotTime carries the top-level "time"
 * value from the snapshot so consumers know when this observation was taken.
 */
public class AircraftPosition {

    private String icao24;
    private String callsign;
    private String originCountry;
    private Long timePosition;
    private Long lastContact;
    private Double longitude;
    private Double latitude;
    private Double baroAltitude;
    private Boolean onGround;
    private Double velocity;
    private Double trueTrack;
    private Double verticalRate;
    private Double geoAltitude;
    private String squawk;
    private Boolean spi;
    private Integer positionSource;
    private Integer category;
    private Long snapshotTime;

    public AircraftPosition() {
    }

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

    public Double getBaroAltitude() { return baroAltitude; }
    public void setBaroAltitude(Double baroAltitude) { this.baroAltitude = baroAltitude; }

    public Boolean getOnGround() { return onGround; }
    public void setOnGround(Boolean onGround) { this.onGround = onGround; }

    public Double getVelocity() { return velocity; }
    public void setVelocity(Double velocity) { this.velocity = velocity; }

    public Double getTrueTrack() { return trueTrack; }
    public void setTrueTrack(Double trueTrack) { this.trueTrack = trueTrack; }

    public Double getVerticalRate() { return verticalRate; }
    public void setVerticalRate(Double verticalRate) { this.verticalRate = verticalRate; }

    public Double getGeoAltitude() { return geoAltitude; }
    public void setGeoAltitude(Double geoAltitude) { this.geoAltitude = geoAltitude; }

    public String getSquawk() { return squawk; }
    public void setSquawk(String squawk) { this.squawk = squawk; }

    public Boolean getSpi() { return spi; }
    public void setSpi(Boolean spi) { this.spi = spi; }

    public Integer getPositionSource() { return positionSource; }
    public void setPositionSource(Integer positionSource) { this.positionSource = positionSource; }

    public Integer getCategory() { return category; }
    public void setCategory(Integer category) { this.category = category; }

    public Long getSnapshotTime() { return snapshotTime; }
    public void setSnapshotTime(Long snapshotTime) { this.snapshotTime = snapshotTime; }
}
