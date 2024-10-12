package com.luna.app.session;

import com.azure.core.util.BinaryData;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusSenderAsyncClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class MessageSessionExample {

    private static final Logger logger = LoggerFactory.getLogger(MessageSessionExample.class);

    // Use environment variables for sensitive information
    private static final String CONNECTION_STRING = System.getenv("AZURE_SERVICE_BUS_CONNECTION_STRING");
    private static final String QUEUE_NAME = "queue001";

    public static void main(String[] args) throws InterruptedException {
        String sessionId1 = "hello-session1";
        String sessionId2 = "hello-session2";

        sendMessagesToSession(sessionId1);
        sendMessagesToSession(sessionId2);
    }

    /**
     * Sends messages to the Service Bus queue with a specific session ID.
     *
     * @param sessionId the session ID to set for the messages
     */
    private static void sendMessagesToSession(String sessionId) throws InterruptedException {
        AtomicBoolean isSuccessful = new AtomicBoolean(false);
        CountDownLatch countdownLatch = new CountDownLatch(1);

        // Create an asynchronous sender client
        try (ServiceBusSenderAsyncClient senderClient = new ServiceBusClientBuilder()
                .connectionString(CONNECTION_STRING)
                .sender()
                .queueName(QUEUE_NAME)
                .buildAsyncClient()) {

            // Create messages with the session ID
            List<ServiceBusMessage> messages = Arrays.asList(
                    createMessageWithSession(sessionId, "Hello1"),
                    createMessageWithSession(sessionId, "Hello2")
            );

            // Send messages asynchronously
            senderClient.sendMessages(messages).subscribe(
                    unused -> logger.info("Batch of messages sent for session: {}", sessionId),
                    error -> logger.error("Error occurred while sending messages for session {}: {}", sessionId, error.getMessage()),
                    () -> {
                        logger.info("Message batch for session {} completed successfully.", sessionId);
                        isSuccessful.set(true);
                    });

            // Wait for the sending process to complete
            countdownLatch.await(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error("Error occurred while closing sender client: {}", e.getMessage(), e);
        }

        logger.info("Message session method completed for session: {}", sessionId);
    }

    /**
     * Creates a ServiceBusMessage with a specific session ID and message body.
     *
     * @param sessionId   the session ID to set
     * @param messageBody the message content
     * @return a ServiceBusMessage with the specified session ID
     */
    private static ServiceBusMessage createMessageWithSession(String sessionId, String messageBody) {
        return new ServiceBusMessage(BinaryData.fromBytes((sessionId + messageBody).getBytes(StandardCharsets.UTF_8)))
                .setSessionId(sessionId);
    }
}
