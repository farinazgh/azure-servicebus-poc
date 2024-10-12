package com.luna.app.deduplicate;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.azure.messaging.servicebus.ServiceBusMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class DuplicateDetectionExample {

    private static final Logger logger = LoggerFactory.getLogger(DuplicateDetectionExample.class);

    // Use environment variables for sensitive information
    private static final String CONNECTION_STRING = System.getenv("AZURE_SERVICE_BUS_CONNECTION_STRING");

    private static final String QUEUE_DUP001 = "dup001";
    private static final String QUEUE_DUP002 = "dup002";
    private static final String QUEUE_DUP003 = "dup003";

    public static void main(String[] args) {
        DuplicateDetectionExample example = new DuplicateDetectionExample();
        example.sendWithoutDuplication(QUEUE_DUP001);
        example.sendWithDuplicationWithoutMessageId(QUEUE_DUP002);
        example.sendWithDuplicationWithMessageId(QUEUE_DUP003);
    }

    /**
     * Sends messages to a queue without duplication.
     */
    public void sendWithoutDuplication(String queueName) {
        try (ServiceBusSenderClient senderClient = createSenderClient(queueName)) {
            sendMessages(senderClient, "message 001", "message 002");
            logger.info("Messages sent to queue: {}", queueName);
        } catch (Exception e) {
            logger.error("Error sending messages to queue {}: {}", queueName, e.getMessage(), e);
        }
    }

    /**
     * Sends messages with duplication but without message IDs.
     */
    public void sendWithDuplicationWithoutMessageId(String queueName) {
        try (ServiceBusSenderClient senderClient = createSenderClient(queueName)) {
            sendMessages(senderClient, "message 001", "message 002");
            logger.info("Messages sent to queue: {}", queueName);
        } catch (Exception e) {
            logger.error("Error sending messages to queue {}: {}", queueName, e.getMessage(), e);
        }
    }

    /**
     * Sends messages with duplication and identical message IDs.
     */
    public void sendWithDuplicationWithMessageId(String queueName) {
        try (ServiceBusSenderClient senderClient = createSenderClient(queueName)) {
            String messageId = UUID.randomUUID().toString();
            logger.info("Using messageId: {}", messageId);
            sendMessagesWithId(senderClient, messageId, "message 001", "message 002");
            logger.info("Messages sent to queue: {}", queueName);
        } catch (Exception e) {
            logger.error("Error sending messages to queue {}: {}", queueName, e.getMessage(), e);
        }
    }

    /**
     * Creates a Service Bus sender client for the specified queue.
     *
     * @param queueName the name of the queue
     * @return the Service Bus sender client
     */
    private ServiceBusSenderClient createSenderClient(String queueName) {
        return new ServiceBusClientBuilder()
                .connectionString(CONNECTION_STRING)
                .sender()
                .queueName(queueName)
                .buildClient();
    }

    /**
     * Sends messages to the queue.
     *
     * @param senderClient the Service Bus sender client
     * @param messages     the messages to send
     */
    private void sendMessages(ServiceBusSenderClient senderClient, String... messages) {
        for (String message : messages) {
            senderClient.sendMessage(new ServiceBusMessage(message));
        }
    }

    /**
     * Sends messages with the same message ID to the queue.
     *
     * @param senderClient the Service Bus sender client
     * @param messageId    the message ID to use
     * @param messages     the messages to send
     */
    private void sendMessagesWithId(ServiceBusSenderClient senderClient, String messageId, String... messages) {
        for (String message : messages) {
            senderClient.sendMessage(new ServiceBusMessage(message).setMessageId(messageId));
        }
    }
}
