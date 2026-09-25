package co.wethinkcode.trafficflow;

import co.wethinkcode.trafficflow.mq.MqConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.time.Instant;
import java.util.Map;

public class RoutingServiceApp {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Latest congestion level received from congestion-service. */
    private static volatile int congestionLevel = 0;

    /** When we last heard from the congestion topic. */
    private static volatile Instant lastUpdate = null;

    public static void main(String[] args) throws Exception {
        startCongestionSubscriber();
        Javalin app = Javalin.create().start(7023);

        app.get("/health", ctx -> ctx.result("OK"));

        app.get("/congestion", ctx -> ctx.json(Map.of(
                "level", congestionLevel,
                "lastUpdate", lastUpdate == null ? null : lastUpdate.toString()
        )));

        app.get("/route", ctx -> {
            String from = ctx.queryParam("from");
            String to = ctx.queryParam("to");
            if (from == null || to == null) {
                ctx.status(400).result("from and to query params required");
                return;
            }
            ctx.status(501).result("Not implemented — needs base-time data + formula");

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
        ActiveMQConnectionFactory factory =
                new ActiveMQConnectionFactory(MqConfig.BROKER_URL);

        Connection conn = factory.createConnection();
        conn.start();

        Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic(MqConfig.TOPIC);
        MessageConsumer consumer = session.createConsumer(topic);

        consumer.setMessageListener(msg -> {
            try {
                if (msg instanceof TextMessage tm) {
                    JsonNode node = MAPPER.readTree(tm.getText());
                    congestionLevel = node.path("level").asInt(0);
                    lastUpdate = Instant.now();
                    System.out.println("Congestion update: level=" + congestionLevel);
                }
            } catch (Exception e) {
                System.err.println("Bad congestion message: " + e.getMessage());
            }
        });
    }
}

// MQ TODO: subscribes to ActiveMQ topic MqConfig.TOPIC at MqConfig.BROKER_URL (see co.wethinkcode.trafficflow.mq.MqConfig)
