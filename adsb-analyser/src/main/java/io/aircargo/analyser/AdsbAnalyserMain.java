package io.aircargo.analyser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.aircargo.analyser.proto.PlaneState;
import io.aircargo.analyser.proto.PositionUpdate;
import io.aircargo.analyser.statemachine.FlightStateMachine;
import io.aircargo.common.model.AircraftPosition;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class AdsbAnalyserMain {

    private static final Logger log = LoggerFactory.getLogger(AdsbAnalyserMain.class);

    private static final String TOPIC_IN          = "adsb.states";
    private static final String TOPIC_TRACKS      = "adsb.tracks";
    private static final String TOPIC_PLANE_STATE = "adsb.plane_state";
    private static final String GROUP_ID          = "adsb-analyser-v2";

    public static void main(String[] args) throws Exception {
        String bootstrapServers = Optional.ofNullable(System.getenv("KAFKA_BOOTSTRAP_SERVERS"))
                .filter(s -> !s.isBlank())
                .orElse("localhost:9092");

        log.info("Starting adsb-analyser  bootstrap={}", bootstrapServers);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(TOPIC_IN)
                .setGroupId(GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<AircraftPosition> positions = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "adsb.states")
                .flatMap(new SnapshotSplitter());

        SingleOutputStreamOperator<PositionUpdate> mainStream = positions
                .keyBy(AircraftPosition::getIcao24)
                .process(new FlightStateMachine());

        DataStream<PlaneState> planeStateStream =
                mainStream.getSideOutput(FlightStateMachine.PLANE_STATE_TAG);

        DataStream<PositionUpdate> tracksLogged = mainStream.map(
                new MapFunction<PositionUpdate, PositionUpdate>() {
                    @Override
                    public PositionUpdate map(PositionUpdate pu) {
                        log.info("TRACK {} | flight={} | phase={} | alt={}m | spd={}m/s | ts={}",
                                pu.getIcao24(), pu.getFlightId(), pu.getFlightPhase().name(),
                                pu.getAltitudeM(), pu.getVelocityMs(), pu.getTimestampMs());
                        return pu;
                    }
                });

        DataStream<PlaneState> statesLogged = planeStateStream.map(
                new MapFunction<PlaneState, PlaneState>() {
                    @Override
                    public PlaneState map(PlaneState ps) {
                        log.info("STATE {} | phase={} | alt={}m | spd={}m/s | dep={} | score={}",
                                ps.getIcao24(), ps.getFlightPhase().name(),
                                ps.getAltitudeM(), ps.getVelocityMs(), ps.getDepartedFrom(), ps.getAnomalyScore());
                        return ps;
                    }
                });

        tracksLogged.sinkTo(KafkaSink.<PositionUpdate>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(new PositionUpdateSerializer(TOPIC_TRACKS))
                .build());

        statesLogged.sinkTo(KafkaSink.<PlaneState>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(new PlaneStateSerializer(TOPIC_PLANE_STATE))
                .build());

        env.execute("adsb-analyser");
    }

    // ---------------------------------------------------------------------------
    // FlatMapFunction: one snapshot → many AircraftPosition records
    // ---------------------------------------------------------------------------

    static class SnapshotSplitter implements FlatMapFunction<String, AircraftPosition> {

        private transient ObjectMapper mapper;

        @Override
        public void flatMap(String value, Collector<AircraftPosition> out) throws Exception {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }

            JsonNode root;
            try {
                root = mapper.readTree(value);
            } catch (Exception e) {
                log.warn("Skipping unparseable snapshot: {}", e.getMessage());
                return;
            }

            JsonNode timeNode = root.get("time");
            Long snapshotTime = (timeNode != null && !timeNode.isNull()) ? timeNode.asLong() : null;

            JsonNode states = root.get("states");
            if (states == null || !states.isArray()) {
                return;
            }

            for (JsonNode stateNode : states) {
                if (!stateNode.isArray()) {
                    continue;
                }

                String icao24 = textOrNull(stateNode, 0);
                if (icao24 == null || icao24.isBlank()) {
                    continue;
                }

                Double longitude = doubleOrNull(stateNode, 5);
                Double latitude  = doubleOrNull(stateNode, 6);
                if (longitude == null || latitude == null) {
                    continue;
                }

                AircraftPosition pos = new AircraftPosition();
                pos.setIcao24(icao24);
                pos.setCallsign(trimmedOrNull(textOrNull(stateNode, 1)));
                pos.setOriginCountry(textOrNull(stateNode, 2));
                pos.setTimePosition(longOrNull(stateNode, 3));
                pos.setLastContact(longOrNull(stateNode, 4));
                pos.setLongitude(longitude);
                pos.setLatitude(latitude);
                pos.setBaroAltitudeM(doubleOrNull(stateNode, 7));
                pos.setOnGround(boolOrNull(stateNode, 8));
                pos.setVelocityMs(doubleOrNull(stateNode, 9));
                pos.setHeadingDeg(doubleOrNull(stateNode, 10));
                pos.setVerticalRateMs(doubleOrNull(stateNode, 11));
                // index 12: sensors — skipped
                pos.setGeoAltitudeM(doubleOrNull(stateNode, 13));
                pos.setSquawk(textOrNull(stateNode, 14));
                pos.setSpi(boolOrNull(stateNode, 15));
                pos.setPositionSource(intOrNull(stateNode, 16));
                // index 17: category — only present when extended=1
                pos.setCategory(intOrNull(stateNode, 17));
                pos.setSnapshotTimeMs(snapshotTime != null ? snapshotTime * 1000L : null);

                out.collect(pos);
            }
        }

        private String textOrNull(JsonNode array, int index) {
            if (index >= array.size()) return null;
            JsonNode n = array.get(index);
            return (n == null || n.isNull()) ? null : n.asText();
        }

        private String trimmedOrNull(String s) {
            if (s == null) return null;
            String t = s.trim();
            return t.isEmpty() ? null : t;
        }

        private Long longOrNull(JsonNode array, int index) {
            if (index >= array.size()) return null;
            JsonNode n = array.get(index);
            return (n == null || n.isNull()) ? null : n.asLong();
        }

        private Double doubleOrNull(JsonNode array, int index) {
            if (index >= array.size()) return null;
            JsonNode n = array.get(index);
            return (n == null || n.isNull()) ? null : n.asDouble();
        }

        private Boolean boolOrNull(JsonNode array, int index) {
            if (index >= array.size()) return null;
            JsonNode n = array.get(index);
            return (n == null || n.isNull()) ? null : n.asBoolean();
        }

        private Integer intOrNull(JsonNode array, int index) {
            if (index >= array.size()) return null;
            JsonNode n = array.get(index);
            return (n == null || n.isNull()) ? null : n.asInt();
        }
    }

    // ---------------------------------------------------------------------------
    // Kafka serialisers — Protobuf bytes
    // ---------------------------------------------------------------------------

    static class PositionUpdateSerializer
            implements KafkaRecordSerializationSchema<PositionUpdate> {

        private final String topic;

        PositionUpdateSerializer(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                PositionUpdate element,
                KafkaSinkContext context,
                Long timestamp) {
            byte[] key   = element.getIcao24().getBytes(StandardCharsets.UTF_8);
            byte[] value = element.toByteArray();
            return new ProducerRecord<>(topic, key, value);
        }
    }

    static class PlaneStateSerializer
            implements KafkaRecordSerializationSchema<PlaneState> {

        private final String topic;

        PlaneStateSerializer(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                PlaneState element,
                KafkaSinkContext context,
                Long timestamp) {
            byte[] key   = element.getIcao24().getBytes(StandardCharsets.UTF_8);
            byte[] value = element.toByteArray();
            return new ProducerRecord<>(topic, key, value);
        }
    }
}
