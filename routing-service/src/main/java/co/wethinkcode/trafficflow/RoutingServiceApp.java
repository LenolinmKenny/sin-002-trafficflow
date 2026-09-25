package co.wethinkcode.trafficflow;

import co.wethinkcode.trafficflow.mq.MqConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.lang.IllegalStateException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

public class RoutingServiceApp {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2))
            .build();

    private static final String INTERSECTION_URL = "http://localhost:7021";
    private static final String CONGESTION_URL   = "http://localhost:7022";

    /** Base travel time in minutes, single-hop, before congestion. */
    private static final double BASE_MINUTES = 10.0;

    /** Multiplier per congestion level. 0 → 1.0x, 8 → 2.2x. */
    private static final double CONGESTION_FACTOR = 0.15;

    /** Latest congestion level received from congestion-topic. -1 = never received. */
    private static volatile int mqCongestionLevel = -1;

    public static void main(String[] args) throws Exception {
        startCongestionSubscriber();
        Javalin app = Javalin.create().start(7023);

        app.get("/health", ctx -> ctx.result("OK"));

        app.get("/congestion", ctx -> ctx.json(Map.of(
                "level", mqCongestionLevel,
                "source", mqCongestionLevel >= 0 ? "mq" : "none"
        )));

        app.get("/route", ctx -> {
            String from = ctx.queryParam("from");
            String to = ctx.queryParam("to");
            if (from == null || to == null) {
                ctx.status(400).result("from and to query params required");
                return;
            }
            try {
                if (!isValidIntersection(from)) {
                    ctx.status(404).json(Map.of("error", "Unknown intersection: " + from));
                    return;
                }
                if (!isValidIntersection(to)) {
                    ctx.status(404).json(Map.of("error", "Unknown intersection: " + to));
                    return;
                }
            } catch (Exception e) {
                ctx.status(503).json(Map.of("error",
                        "intersection-service unreachable: " + e.getMessage()));
                return;
            }

            int level;
            String source;
            if (mqCongestionLevel >= 0) {
                level = mqCongestionLevel;
                source = "mq";
            } else {
                try {
                    level = fetchCongestionOverHttp();
                    source = "http";
                } catch (Exception e) {
                    ctx.status(503).json(Map.of("error",
                            "congestion-service unreachable: " + e.getMessage()));
                    return;
                }
            }

            double minutes = BASE_MINUTES * (1.0 + level * CONGESTION_FACTOR);

            ctx.json(Map.of(
                    "from", from,
                    "to", to,
                    "congestionLevel", level,
                    "congestionSource", source,
                    "estimatedMinutes", Math.round(minutes * 10) / 10.0,
                    "timestamp", Instant.now().toString()
            ));


            // TODO (Provides estimated travel times based on congestion and intersection.)
            // Add domain endpoints for routing-service here.
        });
    }
    /**
     * Subscribe to congestion-topic and cache the latest level.
     * Runs for the lifetime of the app — the connection is deliberately not
     * closed via try-with-resources.
     */
    private static void startCongestionSubscriber() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(MqConfig.BROKER_URL);
        Connection conn = factory.createConnection();
        conn.start();

        Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic(MqConfig.TOPIC);
        MessageConsumer consumer = session.createConsumer(topic);

        consumer.setMessageListener(msg -> {
            try {
                if (msg instanceof TextMessage tm) {
                    JsonNode node = MAPPER.readTree(tm.getText());
                    mqCongestionLevel = node.path("level").asInt(0);
                    System.out.println("Congestion update (mq): level=" + mqCongestionLevel);
                }
            } catch (Exception e) {
                System.err.println("Bad congestion message: " + e.getMessage());
            }
        });
    }

    /** Returns true if intersection-service recognises the ID. */
    private static boolean isValidIntersection(String id) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(
                URI.create(INTERSECTION_URL + "/validate/intersection/" + id)).GET().build();
        HttpResponse<String> res = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() != 200) return false;
        JsonNode node = MAPPER.readTree(res.body());
        return node.path("valid").asBoolean(false);
    }

    /** HTTP fallback when no MQ beat has arrived yet. */
    private static int fetchCongestionOverHttp() throws Exception {
        HttpRequest req = HttpRequest.newBuilder(
                URI.create(CONGESTION_URL + "/congestion")).GET().build();
        HttpResponse<String> res = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() != 200) {
            throw new IllegalStateException("status " + res.statusCode());
        }
        return MAPPER.readTree(res.body()).path("level").asInt(0);
    }
}

// MQ TODO: subscribes to ActiveMQ topic MqConfig.TOPIC at MqConfig.BROKER_URL (see co.wethinkcode.trafficflow.mq.MqConfig)
