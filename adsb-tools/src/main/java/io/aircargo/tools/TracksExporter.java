package io.aircargo.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.aircargo.analyser.proto.PositionUpdate;
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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class TracksExporter {

    private static final Logger log = LoggerFactory.getLogger(TracksExporter.class);

    private static final String TOPIC       = "adsb.tracks";
    private static final String GROUP_ID    = "adsb-tools-tracks-exporter";
    private static final String OUTPUT_PATH = "output/tracks.geojson";

    public static void main(String[] args) throws Exception {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        log.info("Connecting to Kafka at {}", bootstrapServers);

        List<PositionUpdate> updates;
        try (KafkaConsumer<String, byte[]> consumer = buildConsumer(bootstrapServers)) {
            updates = pollAll(consumer);
        }

        Map<String, List<PositionUpdate>> groups = groupAndSort(updates);
        ObjectNode featureCollection = buildFeatureCollection(groups);
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

    private static List<PositionUpdate> pollAll(KafkaConsumer<String, byte[]> consumer) {
        List<TopicPartition> partitions = consumer
                .partitionsFor(TOPIC)
                .stream()
                .map(p -> new TopicPartition(TOPIC, p.partition()))
                .toList();

        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);

        for (TopicPartition p : partitions) {
            log.info("Partition {} starting at offset {}", p.partition(), consumer.position(p));
        }

        List<PositionUpdate> updates = new ArrayList<>();
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
                    updates.add(PositionUpdate.parseFrom(record.value()));
                } catch (Exception e) {
                    log.warn("Failed to deserialise record at offset {} key={}: {}",
                            record.offset(), record.key(), e.getMessage());
                }
            }
        }

        log.info("Read {} records from {}", readCount, TOPIC);
        return updates;
    }

    private static Map<String, List<PositionUpdate>> groupAndSort(List<PositionUpdate> updates) {
        Map<String, List<PositionUpdate>> groups = new LinkedHashMap<>();

        for (PositionUpdate update : updates) {
            String flightId = update.getFlightId();
            if (flightId == null || flightId.isBlank()) {
                continue;
            }
            groups.computeIfAbsent(flightId, k -> new ArrayList<>()).add(update);
        }

        for (List<PositionUpdate> positions : groups.values()) {
            positions.sort(Comparator.comparingLong(PositionUpdate::getTimestampMs));
        }

        return groups;
    }

    private static ObjectNode buildFeatureCollection(Map<String, List<PositionUpdate>> groups) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode collection = mapper.createObjectNode();
        collection.put("type", "FeatureCollection");
        ArrayNode features = collection.putArray("features");

        int written = 0;
        int skipped = 0;

        for (Map.Entry<String, List<PositionUpdate>> entry : groups.entrySet()) {
            String flightId = entry.getKey();
            List<PositionUpdate> positions = entry.getValue();

            if (positions.size() < 2) {
                skipped++;
                continue;
            }

            PositionUpdate first = positions.get(0);
            PositionUpdate last  = positions.get(positions.size() - 1);

            ObjectNode feature = mapper.createObjectNode();
            feature.put("type", "Feature");

            ObjectNode geometry = feature.putObject("geometry");
            geometry.put("type", "LineString");
            ArrayNode coordinates = geometry.putArray("coordinates");
            for (PositionUpdate pos : positions) {
                ArrayNode coord = coordinates.addArray();
                coord.add(pos.getLongitude());
                coord.add(pos.getLatitude());
            }

            ObjectNode props = feature.putObject("properties");
            props.put("flight_id",     flightId);
            props.put("icao24",        first.getIcao24());
            props.put("first_seen_ms", first.getTimestampMs());
            props.put("last_seen_ms",  last.getTimestampMs());
            props.put("point_count",   positions.size());
            props.put("flight_phase",  last.getFlightPhase().name());

            features.add(feature);
            written++;
        }

        log.info("flight_ids read={} written={} skipped_insufficient_points={}",
                groups.size(), written, skipped);
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
