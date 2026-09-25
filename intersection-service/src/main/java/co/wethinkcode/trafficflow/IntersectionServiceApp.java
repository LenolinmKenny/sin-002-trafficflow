package co.wethinkcode.trafficflow;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import co.wethinkcode.trafficflow.mq.MqConfig;
import io.javalin.Javalin;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class IntersectionServiceApp {

    public record Intersection(String id, String district, String signalType, boolean active) {}

    private static final String INGESTION_URL = "http://localhost:7020/intersections";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** districts*/
    private static final Set<String> VALID_DISTRICTS = ConcurrentHashMap.newKeySet();

    /** intersection IDs. */
    private static final Set<String> VALID_IDS = ConcurrentHashMap.newKeySet();

    public static void main(String[] args) {
        Javalin app = Javalin.create().start(7021);

        app.get("/health", ctx -> ctx.result("OK"));

        // TODO (Validates intersection/district names (source of truth).)
        // Add domain endpoints for intersection-service here.

        // Validate a district name against the current known set.
        app.get("/validate/district/{name}", ctx -> {
            String name = ctx.pathParam("name");
            ctx.json(Map.of("name", name, "valid", VALID_DISTRICTS.contains(name)));
        });

        // Validate an intersection ID.
        app.get("/validate/intersection/{id}", ctx -> {
            String id = ctx.pathParam("id");
            ctx.json(Map.of("id", id, "valid", VALID_IDS.contains(id)));
        });

        // Re-pull the source of truth on demand.
        app.post("/refresh", ctx -> {
            refreshFromIngestion();
            ctx.json(Map.of("districts", VALID_DISTRICTS.size(), "ids", VALID_IDS.size()));
        });

        startHeartbeat();
    }

    /** Pull cleaned intersections from the ingestion service and cache the valid names. */
    private static void refreshFromIngestion() throws Exception {
        HttpClient http = HttpClient.newHttpClient();
        HttpRequest req = HttpRequest.newBuilder(URI.create(INGESTION_URL)).GET().build();
        HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());

        if (res.statusCode() != 200) {
            throw new IllegalStateException(
                    "Ingestion service returned " + res.statusCode() + ": " + res.body());
        }

        List<Intersection> records = MAPPER.readValue(
                res.body(), new TypeReference<List<Intersection>>() {});

        VALID_DISTRICTS.clear();
        VALID_IDS.clear();
        for (Intersection r : records) {
            if (r.id() != null) VALID_IDS.add(r.id());
            if (r.district() != null) VALID_DISTRICTS.add(r.district());
        }
        System.out.printf("Refreshed: %d ids, %d districts%n",
                VALID_IDS.size(), VALID_DISTRICTS.size());
    }

    /** Publish a heartbeat to the queue every 30s. */
    private static void startHeartbeat() {
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            try {
                publishHeartbeat();
            } catch (Exception e) {
                System.err.println("Heartbeat failed: " + e.getMessage());
            }
        }, 0, 30, TimeUnit.SECONDS);
    }

    private static void publishHeartbeat() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(MqConfig.BROKER_URL);

        String payload = MAPPER.writeValueAsString(Map.of(
                "service", "intersection-service",
                "timestamp", Instant.now().toString(),
                "knownIds", VALID_IDS.size(),
                "knownDistricts", VALID_DISTRICTS.size()
        ));

        try (Connection conn = factory.createConnection()) {
            conn.start();
            try (Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                var queue = session.createQueue(MqConfig.HEARTBEAT_QUEUE);
                try (MessageProducer producer = session.createProducer(queue)) {
                    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                    TextMessage msg = session.createTextMessage(payload);
                    producer.send(msg);
                }
            }
        }
    }
}

// MQ TODO: publishes a periodic heartbeat to ActiveMQ queue MqConfig.HEARTBEAT_QUEUE at
// MqConfig.BROKER_URL (see co.wethinkcode.trafficflow.mq.MqConfig), consumed by intersection-watchdog.
