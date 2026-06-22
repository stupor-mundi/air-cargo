package io.aircargo.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.aircargo.analyser.proto.PlaneState;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class PlaneStateExporter {

    private static final Logger log = LoggerFactory.getLogger(PlaneStateExporter.class);

    private static final String TOPIC = "adsb.plane_state";
    private static final String GROUP_ID = "adsb-tools-exporter";
    private static final String OUTPUT_PATH = "output/plane_state.geojson";

    public static void main(String[] args) throws Exception {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        log.info("Connecting to Kafka at {}", bootstrapServers);

        List<PlaneState> states;
        try (KafkaConsumer<String, byte[]> consumer = buildConsumer(bootstrapServers)) {
            states = pollAll(consumer);
        }

        ObjectNode featureCollection = buildFeatureCollection(states);
        writeGeoJson(featureCollection);
    }

    private static KafkaConsumer<String, byte[]> buildConsumer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    private static List<PlaneState> pollAll(KafkaConsumer<String, byte[]> consumer) throws Exception {
        // Get partitions explicitly and seek to beginning
        List<org.apache.kafka.common.TopicPartition> partitions = consumer
                .partitionsFor(TOPIC)
                .stream()
                .map(p -> new org.apache.kafka.common.TopicPartition(TOPIC, p.partition()))
                .toList();

        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);

        // confirm positions
        for (TopicPartition p : partitions) {
            log.info("Partition {} starting at offset {}", p.partition(), consumer.position(p));
        }


        List<PlaneState> states = new ArrayList<>();
        int readCount = 0;

        log.info("Reading from topic {} (will stop after 3s with no new messages)", TOPIC);
        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(3));
            log.info("Poll returned {} records", records.count());
            if (records.isEmpty()) {
                break;
            }
            for (ConsumerRecord<String, byte[]> record : records) {
                readCount++;
                try {
                    states.add(PlaneState.parseFrom(record.value()));
                } catch (Exception e) {
                    log.warn("Failed to deserialise record at offset {} key={}: {}",
                            record.offset(), record.key(), e.getMessage());
                }
            }
        }

        log.info("Read {} records from {}", readCount, TOPIC);
        return states;
    }

    private static ObjectNode buildFeatureCollection(List<PlaneState> states) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode collection = mapper.createObjectNode();
        collection.put("type", "FeatureCollection");
        ArrayNode features = collection.putArray("features");

        int written = 0;
        for (PlaneState state : states) {
            if (state.getLatitude() == 0.0 || state.getLongitude() == 0.0) {
                continue;
            }

            ObjectNode feature = mapper.createObjectNode();
            feature.put("type", "Feature");

            ObjectNode geometry = feature.putObject("geometry");
            geometry.put("type", "Point");
            ArrayNode coords = geometry.putArray("coordinates");
            coords.add(state.getLongitude());
            coords.add(state.getLatitude());

            ObjectNode props = feature.putObject("properties");
            props.put("icao24", state.getIcao24());
            props.put("flight_phase", state.getFlightPhase().name());
            props.put("altitude_m", state.getAltitudeM());
            props.put("velocity_ms", state.getVelocityMs());
            props.put("heading_deg", state.getHeadingDeg());
            String flightId = state.getCurrentFlightId();
            if (flightId == null || flightId.isBlank()) {
                props.putNull("callsign");
            } else {
                props.put("callsign", flightId);
            }
            props.put("anomaly_score", state.getAnomalyScore());
            props.put("timestamp_ms", state.getTimestampMs());

            features.add(feature);
            written++;
        }

        log.info("Wrote {} features ({} skipped — no valid position)",
                written, states.size() - written);
        return collection;
    }

    private static void writeGeoJson(ObjectNode featureCollection) throws IOException {
        Files.createDirectories(Path.of("output"));
        ObjectMapper mapper = new ObjectMapper();
        mapper.writerWithDefaultPrettyPrinter()
                .writeValue(new File(OUTPUT_PATH), featureCollection);
        log.info("GeoJSON written to {}", OUTPUT_PATH);
    }
}
