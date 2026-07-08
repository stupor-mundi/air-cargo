package io.aircargo.analyser.statemachine;

import java.io.Serializable;
import java.sql.SQLException;

/**
 * Abstraction over PostGIS airport proximity queries.
 *
 * <p>Extends {@link Serializable} so {@link FlightStateMachine}, which holds a
 * reference to this service as a field, can be checkpointed by Flink without
 * any extra configuration. Concrete implementations must likewise be
 * serializable.
 *
 * <p>The {@link #open()} and {@link #close()} lifecycle hooks are called by
 * {@link FlightStateMachine} during operator setup and teardown. Default
 * implementations are no-ops, which is correct for test stubs that do not hold
 * an external connection.
 */
public interface AirportLookupService extends Serializable {

    /**
     * Returns the ICAO ident of the nearest airport within the implementation's
     * configured proximity threshold, or {@code null} if none is found.
     *
     * @param lat WGS-84 latitude in decimal degrees
     * @param lon WGS-84 longitude in decimal degrees
     * @return airport ICAO ident, or {@code null}
     */
    String findNearestAirport(double lat, double lon) throws SQLException;

    /** Called once before the first {@link #findNearestAirport} invocation. */
    default void open() throws Exception {}

    /** Called when the owning operator is being torn down. */
    default void close() throws Exception {}
}
