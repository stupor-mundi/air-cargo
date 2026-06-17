package io.aircargo.analyser.statemachine;

import io.aircargo.analyser.proto.FlightPhase;
import io.aircargo.analyser.proto.PlaneState;
import io.aircargo.common.model.AircraftPosition;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stateful Flink operator that maintains per-aircraft flight state and detects
 * phase transitions from a keyed stream of {@link AircraftPosition} records.
 *
 * <p>Keyed by ICAO24 (String). One {@link FlightState} is held in RocksDB
 * ValueState per aircraft.
 *
 * <p>Processing-time timers are used (not event time) because OpenSky polling
 * gaps mean event-time watermarks advance slowly and would delay LOST_TRACKING
 * detection. A 25-minute timer is registered on every element; {@link #onTimer}
 * transitions to LOST_TRACKING only when the aircraft has not been seen for
 * &gt;20 minutes, making duplicate timer firings harmless.
 *
 * <p>Emits a {@link PlaneState} Protobuf message after every state transition
 * and every position update. Downstream, {@code PlaneState.toByteArray()} is
 * used to serialise to the {@code adsb.plane_state} Kafka topic.
 *
 * <p>{@code aircraft_type} and {@code operator} are left empty pending registry
 * lookup integration.
 */
public class FlightStateMachine
        extends KeyedProcessFunction<String, AircraftPosition, PlaneState> {

    private static final Logger log = LoggerFactory.getLogger(FlightStateMachine.class);

    private static final long LOST_TRACKING_TIMER_MS      = 25 * 60 * 1000L;
    private static final long LOST_TRACKING_AGE_THRESHOLD = 20 * 60 * 1000L;

    private ValueState<FlightState> flightStateHandle;

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    @Override
    public void open(OpenContext openContext) throws Exception {
        flightStateHandle = getRuntimeContext().getState(
                new ValueStateDescriptor<>("flight-state", FlightState.class));
    }

    // ------------------------------------------------------------------
    // Main processing
    // ------------------------------------------------------------------

    @Override
    public void processElement(AircraftPosition pos, Context ctx, Collector<PlaneState> out)
            throws Exception {

        String icao24 = ctx.getCurrentKey();

        FlightState state = flightStateHandle.value();
        if (state == null) {
            state = new FlightState();
        }

        // Capture altitude before update — needed for go-around comparison
        Double prevAltitudeM = state.getLastAltitudeM();

        updateLastKnown(pos, state);

        FlightPhase phase = state.getPhase() != null
                ? state.getPhase()
                : FlightPhase.FLIGHT_PHASE_UNKNOWN;

        if (phase == FlightPhase.FLIGHT_PHASE_UNKNOWN) {

            if (Boolean.FALSE.equals(pos.getOnGround())
                    && pos.getVelocityMs() != null && pos.getVelocityMs() > 20) {
                String flightId = generateFlightId(icao24, pos.getSnapshotTimeMs());
                state.setPhase(FlightPhase.AIRBORNE);
                state.setFlightId(flightId);
                state.setTakeoffMs(pos.getSnapshotTimeMs());
                state.setTakeoffLat(pos.getLatitude());
                state.setTakeoffLon(pos.getLongitude());
                log.info("New flight detected icao24={} flightId={}", icao24, flightId);

            } else if (Boolean.TRUE.equals(pos.getOnGround())) {
                state.setPhase(FlightPhase.ON_GROUND);
            }

        } else if (phase == FlightPhase.AIRBORNE) {

            if (pos.getVerticalRateMs() != null && pos.getVerticalRateMs() < -3
                    && pos.getVelocityMs() != null && pos.getVelocityMs() < 130
                    && pos.getBaroAltitudeM() != null && pos.getBaroAltitudeM() < 3000) {
                state.setPhase(FlightPhase.ON_APPROACH);
            }

        } else if (phase == FlightPhase.ON_APPROACH) {

            if (pos.getBaroAltitudeM() != null && prevAltitudeM != null
                    && pos.getBaroAltitudeM() > prevAltitudeM
                    && pos.getVerticalRateMs() != null && pos.getVerticalRateMs() > 1) {
                state.setPhase(FlightPhase.AIRBORNE);
                log.info("Go-around detected icao24={}", icao24);

            } else if (Boolean.TRUE.equals(pos.getOnGround())
                    || (pos.getVelocityMs() != null && pos.getVelocityMs() < 10
                        && pos.getBaroAltitudeM() != null && pos.getBaroAltitudeM() < 100)) {
                log.info("Landing detected icao24={} flightId={}", icao24, state.getFlightId());
                state.setPhase(FlightPhase.ON_GROUND);
                state.reset();
            }

        } else if (phase == FlightPhase.ON_GROUND) {

            if (pos.getVelocityMs() != null && pos.getVelocityMs() > 30
                    && pos.getVerticalRateMs() != null && pos.getVerticalRateMs() > 2) {
                String flightId = generateFlightId(icao24, pos.getSnapshotTimeMs());
                state.setPhase(FlightPhase.AIRBORNE);
                state.setFlightId(flightId);
                state.setTakeoffMs(pos.getSnapshotTimeMs());
                state.setTakeoffLat(pos.getLatitude());
                state.setTakeoffLon(pos.getLongitude());
                log.info("Takeoff detected icao24={} flightId={}", icao24, flightId);
            }

        } else if (phase == FlightPhase.LOST_TRACKING) {

            handleReappearance(icao24, pos, state);
        }

        // Register processing-time timer — uses Flink timer service time so
        // harness can advance it in tests via setProcessingTime()
        ctx.timerService().registerProcessingTimeTimer(
                ctx.timerService().currentProcessingTime() + LOST_TRACKING_TIMER_MS);

        flightStateHandle.update(state);
        out.collect(buildPlaneState(icao24, pos, state));
    }

    // ------------------------------------------------------------------
    // Timer — LOST_TRACKING detection
    // ------------------------------------------------------------------

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<PlaneState> out)
            throws Exception {

        FlightState state = flightStateHandle.value();
        if (state == null) {
            return;
        }

        FlightPhase phase = state.getPhase();
        if ((phase == FlightPhase.AIRBORNE || phase == FlightPhase.ON_APPROACH)
                && state.getLastSeenMs() != null) {

            long ageMs = timestamp - state.getLastSeenMs();
            if (ageMs > LOST_TRACKING_AGE_THRESHOLD) {
                long ageMinutes = ageMs / 60_000L;
                log.info("Lost tracking icao24={} last seen {} minutes ago",
                        ctx.getCurrentKey(), ageMinutes);
                state.setPhase(FlightPhase.LOST_TRACKING);
                state.setLostTrackingMs(timestamp);
                flightStateHandle.update(state);
                out.collect(buildPlaneState(ctx.getCurrentKey(), null, state));
            }
        }
    }

    // ------------------------------------------------------------------
    // Phase handlers
    // ------------------------------------------------------------------

    private void handleReappearance(String icao24, AircraftPosition pos, FlightState state) {
        long gapMs = 0;
        if (state.getLostTrackingMs() != null && pos.getSnapshotTimeMs() != null) {
            gapMs = pos.getSnapshotTimeMs() - state.getLostTrackingMs();
        }
        long gapMinutes = gapMs / 60_000L;

        double impliedSpeedKmh = 0.0;
        if (state.getLastLat() != null && state.getLastLon() != null
                && pos.getLatitude() != null && pos.getLongitude() != null
                && gapMs > 0) {
            double distKm = haversineDistanceKm(
                    state.getLastLat(), state.getLastLon(),
                    pos.getLatitude(), pos.getLongitude());
            impliedSpeedKmh = distKm / (gapMs / 3_600_000.0);
        }
        boolean plausible = impliedSpeedKmh < 1200.0;

        log.info("Reappearance after LOST_TRACKING icao24={} gap_min={} implied_speed_kmh={} plausible={}",
                icao24, gapMinutes, String.format("%.1f", impliedSpeedKmh), plausible);

        state.setLostTrackingMs(null);

        if (Boolean.TRUE.equals(pos.getOnGround())) {
            state.setPhase(FlightPhase.ON_GROUND);
            state.reset();
        } else {
            // onGround == false or unknown — resume AIRBORNE with existing flightId
            state.setPhase(FlightPhase.AIRBORNE);
        }
    }

    // ------------------------------------------------------------------
    // State helpers
    // ------------------------------------------------------------------

    private void updateLastKnown(AircraftPosition pos, FlightState state) {
        if (pos.getLatitude() != null)       state.setLastLat(pos.getLatitude());
        if (pos.getLongitude() != null)      state.setLastLon(pos.getLongitude());
        if (pos.getBaroAltitudeM() != null)  state.setLastAltitudeM(pos.getBaroAltitudeM());
        if (pos.getVelocityMs() != null)     state.setLastVelocityMs(pos.getVelocityMs());
        if (pos.getVerticalRateMs() != null) state.setLastVerticalRateMs(pos.getVerticalRateMs());
        if (pos.getHeadingDeg() != null)     state.setLastHeadingDeg(pos.getHeadingDeg());
        if (pos.getSnapshotTimeMs() != null) state.setLastSeenMs(pos.getSnapshotTimeMs());
        if (pos.getCallsign() != null)       state.setCurrentCallsign(pos.getCallsign());
    }

    private String generateFlightId(String icao24, Long snapshotTimeMs) {
        return icao24 + "_" + (snapshotTimeMs != null ? snapshotTimeMs : System.currentTimeMillis());
    }

    // ------------------------------------------------------------------
    // Output builder
    // ------------------------------------------------------------------

    /**
     * Builds a {@link PlaneState} from current state and an optional position.
     * When {@code pos} is null (e.g. called from {@link #onTimer}), last-known
     * values from {@code state} are used for position fields.
     */
    private PlaneState buildPlaneState(String icao24, AircraftPosition pos, FlightState state) {
        PlaneState.Builder builder = PlaneState.newBuilder()
                .setIcao24(icao24)
                .setFlightPhase(state.getPhase() != null
                        ? state.getPhase()
                        : FlightPhase.FLIGHT_PHASE_UNKNOWN)
                .setAnomalyScore(state.getAnomalyScore());

        if (state.getFlightId() != null) {
            builder.setCurrentFlightId(state.getFlightId());
        }
        if (state.getTakeoffMs() != null) {
            builder.setDepartedAtMs(state.getTakeoffMs());
        }

        if (pos != null) {
            if (pos.getSnapshotTimeMs() != null) builder.setTimestampMs(pos.getSnapshotTimeMs());
            if (pos.getLatitude() != null)       builder.setLatitude(pos.getLatitude());
            if (pos.getLongitude() != null)      builder.setLongitude(pos.getLongitude());
            if (pos.getBaroAltitudeM() != null)  builder.setAltitudeM(pos.getBaroAltitudeM());
            if (pos.getVelocityMs() != null)     builder.setVelocityMs(pos.getVelocityMs());
            if (pos.getHeadingDeg() != null)     builder.setHeadingDeg(pos.getHeadingDeg());
        } else {
            // Use last-known state fields
            if (state.getLastSeenMs() != null)    builder.setTimestampMs(state.getLastSeenMs());
            if (state.getLastLat() != null)        builder.setLatitude(state.getLastLat());
            if (state.getLastLon() != null)        builder.setLongitude(state.getLastLon());
            if (state.getLastAltitudeM() != null)  builder.setAltitudeM(state.getLastAltitudeM());
            if (state.getLastVelocityMs() != null) builder.setVelocityMs(state.getLastVelocityMs());
            if (state.getLastHeadingDeg() != null) builder.setHeadingDeg(state.getLastHeadingDeg());
        }

        // aircraft_type and operator left empty — registry lookup deferred

        return builder.build();
    }

    // ------------------------------------------------------------------
    // Geometry
    // ------------------------------------------------------------------

    /**
     * Haversine great-circle distance between two WGS-84 points.
     *
     * <p>TODO: replace with ST_DistanceSphere via Sedona once the spatial
     * pipeline is added.
     *
     * @param lat1 latitude of first point, decimal degrees
     * @param lon1 longitude of first point, decimal degrees
     * @param lat2 latitude of second point, decimal degrees
     * @param lon2 longitude of second point, decimal degrees
     * @return distance in kilometres
     */
    static double haversineDistanceKm(double lat1, double lon1,
                                      double lat2, double lon2) {
        final double R = 6371.0;
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }
}
