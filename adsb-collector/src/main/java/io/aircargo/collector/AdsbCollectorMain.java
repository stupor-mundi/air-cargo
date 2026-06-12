package io.aircargo.collector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AdsbCollectorMain {

    private static final Logger log = LoggerFactory.getLogger(AdsbCollectorMain.class);

    private static final String TOKEN_URL =
            "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token";
    private static final String STATES_URL =
            "https://opensky-network.org/api/states/all";

    public static void main(String[] args) throws IOException {
        String clientId     = requireEnv("OPENSKY_CLIENT_ID");
        String clientSecret = requireEnv("OPENSKY_CLIENT_SECRET");

        OkHttpClient http = new OkHttpClient();
        ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

        String accessToken = fetchAccessToken(http, mapper, clientId, clientSecret);
        log.info("Access token obtained successfully");

        String rawJson = fetchStatesAll(http, accessToken);
        log.info("/states/all response received — pretty-printing below");

        JsonNode parsed = mapper.readTree(rawJson);
        System.out.println(mapper.writeValueAsString(parsed));
    }

    private static String fetchAccessToken(
            OkHttpClient http,
            ObjectMapper mapper,
            String clientId,
            String clientSecret) throws IOException {

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
                log.error("Token request failed — HTTP {}: {}", response.code(), body);
                System.exit(1);
            }
            JsonNode json = mapper.readTree(body);
            JsonNode tokenNode = json.get("access_token");
            if (tokenNode == null || tokenNode.isNull()) {
                log.error("Token response did not contain access_token: {}", body);
                System.exit(1);
            }
            return tokenNode.asText();
        }
    }

    private static String fetchStatesAll(OkHttpClient http, String accessToken) throws IOException {
        log.info("Calling GET {}", STATES_URL);

        Request request = new Request.Builder()
                .url(STATES_URL)
                .header("Authorization", "Bearer " + accessToken)
                .get()
                .build();

        try (Response response = http.newCall(request).execute()) {
            String body = response.body() != null ? response.body().string() : "(empty body)";
            if (!response.isSuccessful()) {
                log.error("/states/all request failed — HTTP {}: {}", response.code(), body);
                System.exit(1);
            }
            return body;
        }
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
