package demo;

import io.javalin.Javalin;
import com.rabbitmq.client.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class RabbitMqApplication {

    private static String QUEUE_NAME = "default-queue";
    private static final ConnectionFactory factory = new ConnectionFactory();
    private static final Logger logger = LogManager.getLogger(RabbitMqApplication.class);
    private static final AtomicBoolean consumerRunning = new AtomicBoolean(false);
    private static final ExecutorService executorService = Executors.newSingleThreadExecutor();

    public static void main(String[] args) {
        configureRabbitMq();
        startServer();
    }

    private static void configureRabbitMq() {
        String host = System.getenv().getOrDefault("RABBITMQ_HOST", "localhost");
        String port = System.getenv().getOrDefault("RABBITMQ_PORT", "5672");
        String username = System.getenv().getOrDefault("RABBITMQ_USERNAME", "guest");
        String password = System.getenv().getOrDefault("RABBITMQ_PASSWORD", "guest");
        QUEUE_NAME = System.getenv().getOrDefault("QUEUE_NAME", "default-queue");

        factory.setHost(host);
        factory.setPort(Integer.parseInt(port));
        factory.setUsername(username);
        factory.setPassword(password);
        factory.setAutomaticRecoveryEnabled(true);
    }

    private static void startServer() {
        Javalin app = Javalin.create().start(7000);

        app.post("/send-data", ctx -> handleProducer(ctx.queryParam("type"), ctx.queryParam("message"), ctx));

        app.post("/consume", ctx -> {
            String type = ctx.queryParam("type");
            if (consumerRunning.get()) {
                ctx.result("Consumer is already running.");
                return;
            }
            executorService.submit(() -> startConsumer(ctx, type));
        });

        app.post("/stop-consumer", ctx -> {
            if (!consumerRunning.get()) {
                ctx.result("No consumer is running.");
                return;
            }
            consumerRunning.set(false);
            logger.info("Consumer stopped.");
            ctx.result("Consumer stopped.");
        });

        logger.info("API is running on port 7000.");
    }

    private static void handleProducer(String type, String message, io.javalin.http.Context ctx) {
        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            if (message == null || message.isBlank()) {
                message = "default message from producer";
            }

            if ("HTTP".equalsIgnoreCase(type)) {
                for (int i = 0; i < 100; i++) {
                    channel.basicPublish("", QUEUE_NAME, null, String.format(message+" : "+i+" : " +LocalDateTime.now().toString()).getBytes(StandardCharsets.UTF_8));
                }
                logger.info("Produced 100 messages of type HTTP.");
                ctx.result("Produced 100 messages.");
            } else if ("MESSAGE".equalsIgnoreCase(type)) {
                channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
                logger.info("Produced a single message of type MESSAGE.");
                ctx.result("Produced a single message.");
            } else {
                logger.error("Invalid type. Must be 'HTTP' or 'MESSAGE'.");
                ctx.result("Invalid type. Must be 'HTTP' or 'MESSAGE'.");
            }
        } catch (Exception e) {
            logger.error("Error in producer", e);
            ctx.status(500).result("Error in producer: " + e.getMessage());
        }
    }

    private static void startConsumer(io.javalin.http.Context ctx, String type) {
        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            consumerRunning.set(true);

            logger.info("Consumer started.");
            ctx.result("Consumer started.");

            if ("MESSAGE".equalsIgnoreCase(type)) {
                // Consume only the last message from the queue
                consumeSingleMessage(channel);
            } else if ("HTTP".equalsIgnoreCase(type)) {
                // Consume all messages from the queue and close the connection afterward
                consumeAllMessages(channel);
            } else {
                logger.error("Invalid consume type. Must be 'MESSAGE' or 'HTTP'.");
                ctx.result("Invalid consume type. Must be 'MESSAGE' or 'HTTP'.");
                consumerRunning.set(false);
            }

        } catch (Exception e) {
            logger.error("Error in consumer", e);
        } finally {
            consumerRunning.set(false);
            logger.info("Consumer stopped gracefully.");
        }
    }

    private static void consumeSingleMessage(Channel channel) {
        try {
            GetResponse response = channel.basicGet(QUEUE_NAME, false);
            if (response != null) {
                String message = new String(response.getBody(), StandardCharsets.UTF_8);
                logger.info("Received single message: {}", message);
                channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
            } else {
                logger.info("No message to consume.");
            }
        } catch (Exception e) {
            logger.error("Error consuming single message", e);
        }
    }

    private static void consumeAllMessages(Channel channel) {
        try {
            while (consumerRunning.get()) {
                GetResponse response = channel.basicGet(QUEUE_NAME, false);
                if (response != null) {
                    String message = new String(response.getBody(), StandardCharsets.UTF_8);
                    logger.info("Received message: {}", message);
                    channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
                } else {
                    logger.info("No more messages to consume. Exiting...");
                    break; // Exit when no messages are left.
                }
            }
        } catch (Exception e) {
            logger.error("Error consuming all messages", e);
        }
    }
}
