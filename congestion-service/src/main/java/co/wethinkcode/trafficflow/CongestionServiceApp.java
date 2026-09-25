package co.wethinkcode.trafficflow;

import co.wethinkcode.trafficflow.mq.MqConfig;
import io.javalin.Javalin;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.time.Instant;
import java.util.Map;

public class CongestionServiceApp {

    private static final int MIN_LEVEL = 0;
    private static final int MAX_LEVEL = 8;

    private static volatile int level = 0;

    public static void main(String[] args) {
        Javalin app = Javalin.create().start(7022);

        app.get("/health", ctx -> ctx.result("OK"));

        // TODO (Tracks the city-wide Congestion Level (0-8).)
        // Add domain endpoints for congestion-service here.

        app.get("/congestion", ctx ->
                ctx.json(Map.of("level", level, "timestamp", Instant.now().toString())));

        app.post("/congestion/{level}", ctx -> {
            int newLevel = Integer.parseInt(ctx.pathParam("level"));
            if (newLevel < MIN_LEVEL || newLevel > MAX_LEVEL) {
                ctx.status(400).result("Level must be " + MIN_LEVEL + "–" + MAX_LEVEL);
                return;
            }
            level = newLevel;
            publishCongestion(newLevel);
            ctx.json(Map.of("level", newLevel));
        });
    }
    private static void publishCongestion(int level) throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(MqConfig.BROKER_URL);

        String payload = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(
                Map.of("level", level, "timestamp", Instant.now().toString()));

        try (Connection conn = factory.createConnection()) {
            conn.start();
            try (Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                Topic topic = session.createTopic(MqConfig.TOPIC);
                try (MessageProducer producer = session.createProducer(topic)) {
                    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                    producer.send(session.createTextMessage(payload));
                }
            }
        }
    }
}

// MQ TODO: publishes to ActiveMQ topic MqConfig.TOPIC at MqConfig.BROKER_URL (see co.wethinkcode.trafficflow.mq.MqConfig)
