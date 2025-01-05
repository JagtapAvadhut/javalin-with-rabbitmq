package demo;

import io.javalin.Javalin;
import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
public class RabbitMqApplication {
    private static String QUEUE_NAME = "default-queue";
    private static final ConnectionFactory factory = new ConnectionFactory();
    private static final Logger logger = LogManager.getLogger(RabbitMqApplication.class);
    private static final AtomicBoolean consumerRunning = new AtomicBoolean(false);  // Using AtomicBoolean for thread-safe operation

    public static void main(String[] args) {
        // RabbitMQ connection setup
        String host = System.getenv().getOrDefault("RABBITMQ_HOST", "host.docker.internal");
        String port = System.getenv().getOrDefault("RABBITMQ_PORT", "5672");
        String username = System.getenv().getOrDefault("RABBITMQ_USERNAME", "guest");
        String password = System.getenv().getOrDefault("RABBITMQ_PASSWORD", "guest");
        QUEUE_NAME = System.getenv().getOrDefault("QUEUE_NAME", "default-queue");

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
                    logger.info("Produced 100 messages : {} [type : {} ] ", message,type);
                    ctx.result("Produced 100 messages.");
                } else if ("MESSAGE".equalsIgnoreCase(type)) {
                    channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
                    logger.info("Produced a single message : {} ", message);
                    ctx.result("Produced a single message.");
                } else {
                    logger.error("Invalid type. Please provide type as 'HTTP' or 'MESSAGE'.");
                    ctx.result("Invalid type. Please provide type as 'HTTP' or 'MESSAGE'.");
                }
            } catch (Exception e) {
                logger.error("Error handling /send-data request", e);
                ctx.status(500).result("Internal Server Error" + e.getMessage());
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
            logger.info("Stopping consumer...");
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
                    logger.info("Received message: {}", message);
                    channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
                } else {
                    logger.info("No message to consume. Waiting for next message...");
                    Thread.sleep(1000);
                }
            }
        } catch (Exception e) {
            logger.error("Error consuming message", e);
        }
    }
}