package io.aircargo.collector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AdsbCollectorMain {

    private static final Logger log = LoggerFactory.getLogger(AdsbCollectorMain.class);

    private static final String TOKEN_URL =
            "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token";
    private static final String STATES_URL =
            "https://opensky-network.org/api/states/all?extended=1";
    private static final String TOPIC = "adsb.states";
    private static final long POLL_INTERVAL_MINUTES = 10;
    private static final long TOKEN_REFRESH_MINUTES = 25;

    private final String clientId;
    private final String clientSecret;
    private final OkHttpClient http;
    private final ObjectMapper mapper;
    private final KafkaProducer<String, String> producer;

    private volatile String currentToken;
    private volatile Instant tokenFetchedAt;

    private AdsbCollectorMain(String clientId, String clientSecret) {
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.http = new OkHttpClient();
        this.mapper = new ObjectMapper();
        this.producer = buildProducer();
    }

    public static void main(String[] args) {
        String clientId     = requireEnv("OPENSKY_CLIENT_ID");
        String clientSecret = requireEnv("OPENSKY_CLIENT_SECRET");
        new AdsbCollectorMain(clientId, clientSecret).run();
    }

    private void run() {
        currentToken = fetchAccessToken();
        tokenFetchedAt = Instant.now();
        log.info("Initial OAuth2 token obtained");

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "poll-scheduler");
            t.setDaemon(false);
            return t;
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received — stopping scheduler");
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            producer.close(Duration.ofSeconds(10));
            log.info("Shutdown complete");
        }, "shutdown-hook"));

        scheduler.scheduleAtFixedRate(this::poll, 0, POLL_INTERVAL_MINUTES, TimeUnit.MINUTES);
        log.info("Collector started — polling every {} minutes, token refresh at {} minutes",
                POLL_INTERVAL_MINUTES, TOKEN_REFRESH_MINUTES);
    }

    private void poll() {
        try {
            maybeRefreshToken();

            String rawJson = fetchStatesAll();
            String key = extractTimeKey(rawJson);

            producer.send(new ProducerRecord<>(TOPIC, key, rawJson), (metadata, ex) -> {
                if (ex != null) {
                    log.error("Failed to produce to {}: {}", TOPIC, ex.getMessage(), ex);
                } else {
                    log.info("Produced snapshot → {}  partition={}  offset={}  key={}",
                            TOPIC, metadata.partition(), metadata.offset(), key);
                }
            });
        } catch (Exception e) {
            // Catch and log so the scheduler keeps firing on the next interval
            log.error("Poll cycle failed: {}", e.getMessage(), e);
        }
    }

    private void maybeRefreshToken() {
        if (Duration.between(tokenFetchedAt, Instant.now()).toMinutes() >= TOKEN_REFRESH_MINUTES) {
            log.info("Token age >= {} minutes — refreshing proactively", TOKEN_REFRESH_MINUTES);
            currentToken = fetchAccessToken();
            tokenFetchedAt = Instant.now();
            log.info("Token refreshed");
        }
    }

    private String fetchAccessToken() {
        log.info("Requesting OAuth2 token from {}", TOKEN_URL);
        Request request = new Request.Builder()
                .url(TOKEN_URL)
                .post(new FormBody.Builder()
                        .add("grant_type", "client_credentials")
                        .add("client_id", clientId)
                        .add("client_secret", clientSecret)
                        .build())
                .build();

        try (Response response = http.newCall(request).execute()) {
            String body = response.body() != null ? response.body().string() : "(empty body)";
            if (!response.isSuccessful()) {
                throw new RuntimeException("Token request failed — HTTP " + response.code() + ": " + body);
            }
            JsonNode json = mapper.readTree(body);
            JsonNode tokenNode = json.get("access_token");
            if (tokenNode == null || tokenNode.isNull()) {
                throw new RuntimeException("Token response missing access_token: " + body);
            }
            return tokenNode.asText();
        } catch (IOException e) {
            throw new RuntimeException("Token request IO error: " + e.getMessage(), e);
        }
    }

    private String fetchStatesAll() {
        log.info("Polling GET {}", STATES_URL);
        Request request = new Request.Builder()
                .url(STATES_URL)
                .header("Authorization", "Bearer " + currentToken)
                .get()
                .build();

        try (Response response = http.newCall(request).execute()) {
            String body = response.body() != null ? response.body().string() : "(empty body)";
            if (!response.isSuccessful()) {
                throw new RuntimeException("/states/all failed — HTTP " + response.code() + ": " + body);
            }
            return body;
        } catch (IOException e) {
            throw new RuntimeException("/states/all IO error: " + e.getMessage(), e);
        }
    }

    private String extractTimeKey(String rawJson) {
        try {
            JsonNode root = mapper.readTree(rawJson);
            JsonNode timeNode = root.get("time");
            if (timeNode != null && !timeNode.isNull()) {
                return timeNode.asText();
            }
        } catch (Exception e) {
            log.warn("Could not extract time field from response, using wall clock: {}", e.getMessage());
        }
        return String.valueOf(System.currentTimeMillis() / 1000);
    }

    private static KafkaProducer<String, String> buildProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        return new KafkaProducer<>(props);
    }

    private static String requireEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            log.error("Required environment variable {} is not set", name);
            System.exit(1);
        }
        return value;
    }
}
