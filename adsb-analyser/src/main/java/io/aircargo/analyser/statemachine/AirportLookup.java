package io.aircargo.analyser.statemachine;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Queries PostGIS for the nearest airport within {@link #THRESHOLD_M} metres of
 * a given position. Used on takeoff and landing transitions in
 * {@link FlightStateMachine}.
 *
 * <p>Implements {@link Serializable} so Flink can checkpoint the
 * {@link FlightStateMachine} that holds a reference to this object. The JDBC
 * {@link Connection} is {@code transient} and must be re-opened via {@link #open()}
 * after deserialisation (Flink calls {@code open()} on each TaskManager before
 * the operator starts processing).
 *
 * <p>Airports are expected in the {@code reference.airports} schema. The
 * {@code geom} column must be populated (see {@code 03_load_data.sql}).
 */
public class AirportLookup implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String JDBC_URL   = "jdbc:postgresql://localhost:5432/postgres";
    private static final String JDBC_USER  = "postgres";
    private static final String JDBC_PASS  = "postgres";
    private static final double THRESHOLD_M = 5000.0;

    private transient Connection connection;

    public void open() throws SQLException {
        connection = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
    }

    public void close() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    /**
     * Returns the ICAO ident of the nearest airport within {@link #THRESHOLD_M}
     * metres of the given position, or {@code null} if none is found.
     *
     * <p>Uses {@code ST_DWithin} with a {@code geography} cast for metre-accurate
     * distance filtering, then orders by the KNN {@code <->} operator to pick
     * the single closest match.
     *
     * @param lat WGS-84 latitude in decimal degrees
     * @param lon WGS-84 longitude in decimal degrees
     * @return airport ICAO ident, or {@code null}
     */
    public String findNearestAirport(double lat, double lon) throws SQLException {
        String sql = """
                SELECT ident
                FROM reference.airports
                WHERE ST_DWithin(
                    geom::geography,
                    ST_SetSRID(ST_MakePoint(?, ?), 4326)::geography,
                    ?
                )
                ORDER BY geom::geography <->
                    ST_SetSRID(ST_MakePoint(?, ?), 4326)::geography
                LIMIT 1
                """;
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setDouble(1, lon);
            ps.setDouble(2, lat);
            ps.setDouble(3, THRESHOLD_M);
            ps.setDouble(4, lon);
            ps.setDouble(5, lat);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString("ident") : null;
            }
        }
    }
}
