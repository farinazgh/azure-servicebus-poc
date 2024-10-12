package com.luna.app.filter;

import com.azure.messaging.servicebus.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TopicFilterExample {

    private static final Logger logger = LoggerFactory.getLogger(TopicFilterExample.class);

    // Use environment variables for sensitive information
    private static final String CONNECTION_STRING = System.getenv("AZURE_SERVICE_BUS_CONNECTION_STRING");
    private static final String TOPIC_NAME = "topic001";

    private static final String SUB_NAME1 = "sub1";
    private static final String SUB_NAME2 = "sub2";
    private static final String SUB_NAME3 = "sub3";

    public static void main(String[] args) throws InterruptedException {
        TopicFilterExample example = new TopicFilterExample();
        example.sendMessages();
        example.receiveMessages(SUB_NAME1);
        example.receiveMessages(SUB_NAME2);
        example.receiveMessages(SUB_NAME3);
    }

    /**
     * Sends multiple messages to the Azure Service Bus topic with different application properties.
     */
    private void sendMessages() {
        try (ServiceBusSenderClient senderClient = createSenderClient()) {
            sendMessageWithColor(senderClient, "topic filter example red", "red");
            sendMessageWithColor(senderClient, "topic filter example blue", "blue");
            sendMessageWithColor(senderClient, "topic filter example green", "green");

            logger.info("Sent messages to the topic: {}", TOPIC_NAME);
        } catch (Exception e) {
            logger.error("Error sending messages to topic: {}", e.getMessage(), e);
        }
    }

    /**
     * Creates a Service Bus sender client for the topic.
     *
     * @return the Service Bus sender client
     */
    private ServiceBusSenderClient createSenderClient() {
        return new ServiceBusClientBuilder()
                .connectionString(CONNECTION_STRING)
                .sender()
                .topicName(TOPIC_NAME)
                .buildClient();
    }

    /**
     * Sends a message with a color property to the topic.
     *
     * @param senderClient the Service Bus sender client
     * @param messageBody  the message content
     * @param color        the color property to be set
     */
    private void sendMessageWithColor(ServiceBusSenderClient senderClient, String messageBody, String color) {
        ServiceBusMessage message = new ServiceBusMessage(messageBody);
        message.getApplicationProperties().put("color", color);
        senderClient.sendMessage(message);
        logger.info("Sent message with color: {}", color);
    }

    /**
     * Receives messages from a specific subscription of the Azure Service Bus topic.
     *
     * @param subscriptionName the name of the subscription to receive messages from
     */
    private void receiveMessages(String subscriptionName) throws InterruptedException {
        CountDownLatch countdownLatch = new CountDownLatch(1);

        // Create the processor client for receiving messages
        ServiceBusProcessorClient processorClient = new ServiceBusClientBuilder()
                .connectionString(CONNECTION_STRING)
                .processor()
                .topicName(TOPIC_NAME)
                .subscriptionName(subscriptionName)
                .processMessage(TopicFilterExample::processMessage)
                .processError(context -> processError(context, countdownLatch))
                .buildProcessorClient();

        logger.info("Starting the processor for subscription: {}", subscriptionName);
        processorClient.start();

        // Allow the processor to run for 10 seconds
        TimeUnit.SECONDS.sleep(10);

        logger.info("Stopping the processor for subscription: {}", subscriptionName);
        processorClient.close();
    }

    /**
     * Processes received messages from the subscription.
     *
     * @param context the message context
     */
    private static void processMessage(ServiceBusReceivedMessageContext context) {
        ServiceBusReceivedMessage message = context.getMessage();
        logger.info("Processing message. Session: {}, Sequence #: {}, Contents: {}",
                message.getMessageId(), message.getSequenceNumber(), message.getBody());
    }

    /**
     * Handles errors that occur during message processing.
     *
     * @param context         the error context
     * @param countdownLatch  the latch to signal error handling completion
     */
    private static void processError(ServiceBusErrorContext context, CountDownLatch countdownLatch) {
        logger.error("Error occurred while processing message: {}", context.getException().getMessage());
        countdownLatch.countDown();
    }
}
