package demo;

import io.javalin.Javalin;
import com.rabbitmq.client.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RabbitMqApplication {
    private static final String QUEUE_NAME = "default-queue";
    private static final ConnectionFactory factory = new ConnectionFactory();
    private static final Logger logger = LoggerFactory.getLogger(RabbitMqApplication.class);
    private static final AtomicBoolean consumerRunning = new AtomicBoolean(false);  // Using AtomicBoolean for thread-safe operation

    public static void main(String[] args) {
        // RabbitMQ connection setup
        String host = System.getenv().getOrDefault("RABBITMQ_HOST", "localhost");
        String port = System.getenv().getOrDefault("RABBITMQ_PORT", "5672");
        String username = System.getenv().getOrDefault("RABBITMQ_USERNAME", "guest");
        String password = System.getenv().getOrDefault("RABBITMQ_PASSWORD", "guest");

        factory.setHost(host);
        factory.setPort(Integer.parseInt(port));
        factory.setUsername(username);
        factory.setPassword(password);
        factory.setAutomaticRecoveryEnabled(true);

        // Javalin server
        Javalin app = Javalin.create().start(7000);
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        // Producer endpoint
        app.post("/send-data", ctx -> {
            try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
                channel.queueDeclare(QUEUE_NAME, true, false, false, null);
                String type = ctx.queryParam("type");
                String message = ctx.queryParam("message");
                if (message == null || message.isBlank()) {
                    message = "default message from producer";
                }
                if ("HTTP".equalsIgnoreCase(type)) {
                    for (int i = 0; i < 100; i++) {
                        channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
                    }
                    ctx.result("Produced 100 messages.");
                } else {
                    channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
                    ctx.result("Produced a single message.");
                }
            } catch (Exception e) {
                logger.error("Error handling /send-data request", e);
                ctx.status(500).result("Internal Server Error");
            }
        });

        // Consumer endpoint
        app.post("/consume", ctx -> {
            executorService.submit(() -> {
                try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
                    channel.queueDeclare(QUEUE_NAME, true, false, false, null);
                    consumerRunning.set(true); // Set consumer as running
                    consumeMessages(channel);
                    ctx.result("Consumer started and ready to consume messages.");
                } catch (Exception e) {
                    logger.error("Error starting consumer", e);
                    ctx.status(500).result("Error starting consumer: " + e.getMessage());
                }
            });
        });
        // Stop consumer endpoint
        app.post("/stop-consumer", ctx -> {
            ctx.result("Consumer stopped.");
            while (true) {
                if (!consumerRunning.get()) {
                    System.exit(0);
                    ctx.result("No consumer is running.");
                    return;
                }
                consumerRunning.set(false); // Stop the consumer
            }
        });

        logger.info("API is running on port 7000.");
    }

    private static void consumeMessages(Channel channel) {
        try {
            while (true) {
                GetResponse response = channel.basicGet(QUEUE_NAME, false);
                if (response != null) {
                    String message = new String(response.getBody(), StandardCharsets.UTF_8);
                    System.out.println("Consumed message: " + message);
                    channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
                } else {
                    System.out.println("No message to consume. Waiting for next message...");
                    Thread.sleep(1000);
                }
            }
        } catch (Exception e) {
            logger.error("Error consuming message", e);
        }
    }
}
