package co.wethinkcode.trafficflow;

import co.wethinkcode.trafficflow.mq.MqConfig;
import io.javalin.Javalin;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class IntersectionWatchdogApp {

    /** Alarm if no beat for this long. Should be ~3x the heartbeat interval. */
    private static final Duration TIMEOUT = Duration.ofSeconds(90);

    /** How often the checker runs. Small relative to TIMEOUT. */
    private static final Duration CHECK_INTERVAL = Duration.ofSeconds(10);

    /** Set once at startup, used as the "reference point" before the first beat. */
    private static final Instant STARTED_AT = Instant.now();

    /** When we last heard from intersection-service. null = never. */
    private static volatile Instant lastHeartbeat = null;

    /** Current alarm state — tracked so we log transitions, not every check. */
    private static volatile boolean alerting = false;

    public static void main(String[] args) throws Exception  {
        startHeartbeatSubscriber();
        startWatchdogLoop();

        Javalin app = Javalin.create().start(7024);

        app.get("/health", ctx -> ctx.result("OK"));

        app.get("/status", ctx -> ctx.json(Map.of(
                "intersectionService", alerting ? "DOWN" : "UP",
                "lastHeartbeat", lastHeartbeat == null ? "never" : lastHeartbeat.toString(),
                "timeoutSeconds", TIMEOUT.toSeconds()
        )));

        // TODO (Cries for help if the Intersection Service crashes, since routes can no longer be validated.)
        // Mechanism: ActiveMQ Queue heartbeat/dead-letter
    }
    private static void startHeartbeatSubscriber() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(MqConfig.BROKER_URL);
        Connection conn = factory.createConnection();
        conn.start();

        Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(MqConfig.HEARTBEAT_QUEUE);
        MessageConsumer consumer = session.createConsumer(queue);

        consumer.setMessageListener(msg -> {
            try {
                if (msg instanceof TextMessage tm) {
                    lastHeartbeat = Instant.now();
                    System.out.println("Heartbeat received: " + tm.getText());
                }
            } catch (JMSException e) {
                System.err.println("Bad heartbeat message: " + e.getMessage());
            }
        });
    }

    /**
     * Periodically check whether a beat is overdue. Logs on state transitions only,
     * so you get one [ALERT] when it goes down and one [RECOVERED] when it comes back.
     */
    private static void startWatchdogLoop() {
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            boolean down = isDown();
            if (down && !alerting) {
                alerting = true;
                System.err.println("[ALERT] Intersection Service heartbeat missed — last beat: "
                        + (lastHeartbeat == null ? "never" : lastHeartbeat));
            } else if (!down && alerting) {
                alerting = false;
                System.out.println("[RECOVERED] Intersection Service heartbeat resumed");
            }
        }, 0, CHECK_INTERVAL.toSeconds(), TimeUnit.SECONDS);
    }

    /**
     * We're "down" if the gap since the most recent beat (or, before any beat has
     * ever arrived, since startup) exceeds TIMEOUT.
     */
    private static boolean isDown() {
        Instant last = lastHeartbeat;
        Instant reference = (last != null) ? last : STARTED_AT;
        return Duration.between(reference, Instant.now()).compareTo(TIMEOUT) > 0;
    }
}

// MQ TODO: subscribes to ActiveMQ queue MqConfig.HEARTBEAT_QUEUE at MqConfig.BROKER_URL
// (see co.wethinkcode.trafficflow.mq.MqConfig) and alerts if a heartbeat from
// intersection-service is missed or a message lands in the dead-letter queue.
