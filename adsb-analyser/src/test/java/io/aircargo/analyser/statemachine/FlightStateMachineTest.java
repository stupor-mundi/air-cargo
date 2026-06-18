package io.aircargo.analyser.statemachine;

import io.aircargo.analyser.proto.FlightPhase;
import io.aircargo.analyser.proto.PlaneState;
import io.aircargo.analyser.proto.PositionUpdate;
import io.aircargo.common.model.AircraftPosition;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link FlightStateMachine} using Flink's
 * {@link KeyedOneInputStreamOperatorTestHarness}.
 *
 * <p>Main output: {@link PositionUpdate} — accessed via {@link #allOutputs()} / {@link #lastOutput()}.
 * Side output: {@link PlaneState} — accessed via {@link #allSideOutputs()}.
 *
 * <p>Processing time is controlled via {@code harness.setProcessingTime()} to
 * trigger lost-tracking timers deterministically.
 */
class FlightStateMachineTest {

    private static final String ICAO24 = "abc123";

    private KeyedOneInputStreamOperatorTestHarness<String, AircraftPosition, PositionUpdate> harness;

    @BeforeEach
    void setUp() throws Exception {
        FlightStateMachine fsm = new FlightStateMachine();
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(fsm),
                AircraftPosition::getIcao24,
                Types.STRING);
        harness.open();
        harness.setProcessingTime(0L);
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    // ------------------------------------------------------------------
    // Tests
    // ------------------------------------------------------------------

    /**
     * Aircraft first seen with onGround=false and high velocity while state is
     * UNKNOWN should immediately transition to AIRBORNE and generate a flightId.
     */
    @Test
    void test_unknownToAirborne() throws Exception {
        harness.processElement(buildPos(ICAO24, false, 150.0, 5000.0, 0.0, 1000L), 1000L);

        PositionUpdate last = lastOutput();
        assertNotNull(last, "Expected at least one PositionUpdate output");
        assertEquals(FlightPhase.AIRBORNE, last.getFlightPhase());
        assertFalse(last.getFlightId().isBlank(), "flightId should be set");
    }

    /**
     * Full flight cycle: UNKNOWN → AIRBORNE (cruise) → ON_APPROACH (descending
     * and slowing below 3000m) → ON_GROUND (landed).
     * After landing, flightId on PlaneState side output should be blank.
     */
    @Test
    void test_simpleFlightCycle() throws Exception {
        harness.processElement(buildPos(ICAO24, false, 220.0, 10000.0, 2.0, 1000L), 1000L);
        harness.processElement(buildPos(ICAO24, false, 90.0, 2000.0, -6.0, 2000L), 2000L);
        harness.processElement(buildPos(ICAO24, true, 5.0, 30.0, -1.0, 3000L), 3000L);

        List<PositionUpdate> outputs = allOutputs();
        assertEquals(3, outputs.size(), "Expected one PositionUpdate per position");

        assertEquals(FlightPhase.AIRBORNE,    outputs.get(0).getFlightPhase());
        assertEquals(FlightPhase.ON_APPROACH, outputs.get(1).getFlightPhase());
        assertEquals(FlightPhase.ON_GROUND,   outputs.get(2).getFlightPhase());

        // flightId cleared after reset() on landing — check PlaneState side output
        List<PlaneState> sideOutputs = allSideOutputs();
        assertEquals(3, sideOutputs.size(), "Expected one PlaneState per position");
        assertTrue(sideOutputs.get(2).getCurrentFlightId().isBlank(),
                "PlaneState flightId should be blank after landing");
    }

    /**
     * Go-around: aircraft enters ON_APPROACH then climbs again — should
     * transition back to AIRBORNE without landing.
     */
    @Test
    void test_goAround() throws Exception {
        harness.processElement(buildPos(ICAO24, false, 220.0, 10000.0, 2.0, 1000L), 1000L);
        harness.processElement(buildPos(ICAO24, false, 90.0, 2000.0, -5.0, 2000L), 2000L);
        harness.processElement(buildPos(ICAO24, false, 120.0, 2200.0, 4.0, 3000L), 3000L);

        List<PositionUpdate> outputs = allOutputs();
        assertEquals(3, outputs.size());
        assertEquals(FlightPhase.AIRBORNE,    outputs.get(0).getFlightPhase());
        assertEquals(FlightPhase.ON_APPROACH, outputs.get(1).getFlightPhase());
        assertEquals(FlightPhase.AIRBORNE,    outputs.get(2).getFlightPhase(),
                "Should return to AIRBORNE after go-around");
    }

    /**
     * Aircraft goes LOST_TRACKING (timer fires after 25 minutes of no contact),
     * then reappears still airborne and resumes with the same flightId.
     *
     * <p>LOST_TRACKING appears on the PlaneState side output (emitted from the timer).
     * The resumed AIRBORNE phase appears on the main PositionUpdate output.
     */
    @Test
    void test_lostTrackingReappears() throws Exception {
        // Position 1: airborne at processing time 0, snapshotTimeMs=0
        harness.processElement(buildPos(ICAO24, false, 200.0, 9000.0, 1.0, 0L), 0L);

        String originalFlightId = lastOutput().getFlightId();
        assertFalse(originalFlightId.isBlank(), "flightId should be set before gap");

        // Advance processing time past the 25-minute timer threshold.
        // Timer registered at currentProcessingTime(0) + 25*60*1000 = 1_500_000ms.
        harness.setProcessingTime(26L * 60L * 1000L);

        // Timer fires → LOST_TRACKING emitted on PlaneState side output only.
        // Main output count is still 1 (no PositionUpdate from timer).
        assertEquals(1, allOutputs().size(), "Timer should not add to main output");
        List<PlaneState> sideAfterTimer = allSideOutputs();
        assertEquals(FlightPhase.LOST_TRACKING,
                sideAfterTimer.get(sideAfterTimer.size() - 1).getFlightPhase(),
                "PlaneState side output should show LOST_TRACKING after timer fires");

        // Reappearance: airborne, 60 minutes after start
        harness.processElement(
                buildPos(ICAO24, false, 180.0, 8500.0, 0.5, 60L * 60L * 1000L),
                60L * 60L * 1000L);

        PositionUpdate resumed = lastOutput();
        assertEquals(FlightPhase.AIRBORNE, resumed.getFlightPhase(),
                "Should resume AIRBORNE on reappearance");
        assertEquals(originalFlightId, resumed.getFlightId(),
                "Should resume with the same flightId");
    }

    /**
     * Aircraft first seen already airborne (was in the air before the collector
     * started). UNKNOWN → AIRBORNE should fire on the very first message.
     */
    @Test
    void test_firstSeenAirborne() throws Exception {
        harness.processElement(buildPos(ICAO24, false, 240.0, 11000.0, 1.5, 5000L), 5000L);

        PositionUpdate output = lastOutput();
        assertNotNull(output);
        assertEquals(FlightPhase.AIRBORNE, output.getFlightPhase(),
                "First-ever sighting while airborne should produce AIRBORNE phase");
        assertFalse(output.getFlightId().isBlank(),
                "flightId should be generated on first sighting");
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /**
     * Builds an {@link AircraftPosition} with the given key fields and
     * fixed defaults for lat/lon/heading so tests don't have to specify them.
     */
    private AircraftPosition buildPos(String icao24,
                                      Boolean onGround,
                                      Double velocityMs,
                                      Double altitudeM,
                                      Double verticalRate,
                                      Long snapshotMs) {
        return AircraftPosition.builder()
                .icao24(icao24)
                .onGround(onGround)
                .velocityMs(velocityMs)
                .baroAltitudeM(altitudeM)
                .verticalRateMs(verticalRate)
                .snapshotTimeMs(snapshotMs)
                .latitude(51.5)
                .longitude(-0.1)
                .headingDeg(270.0)
                .build();
    }

    @SuppressWarnings("unchecked")
    private List<PositionUpdate> allOutputs() {
        List<PositionUpdate> result = new ArrayList<>();
        for (Object o : harness.getOutput()) {
            if (o instanceof StreamRecord) {
                result.add((PositionUpdate) ((StreamRecord<?>) o).getValue());
            }
        }
        return result;
    }

    private PositionUpdate lastOutput() {
        List<PositionUpdate> all = allOutputs();
        return all.isEmpty() ? null : all.get(all.size() - 1);
    }

    private List<PlaneState> allSideOutputs() {
        List<PlaneState> result = new ArrayList<>();
        for (StreamRecord<PlaneState> r : harness.getSideOutput(FlightStateMachine.PLANE_STATE_TAG)) {
            result.add(r.getValue());
        }
        return result;
    }
}
