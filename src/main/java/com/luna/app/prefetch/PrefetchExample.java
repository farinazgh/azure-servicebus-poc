package com.luna.app.prefetch;

import com.azure.core.util.IterableStream;
import com.azure.messaging.servicebus.*;
import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;
import com.google.common.base.Stopwatch;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public class PrefetchExample {

    // Use environment variables for sensitive information
    private static final String CONNECTION_STRING = System.getenv("AZURE_SERVICE_BUS_CONNECTION_STRING");
    private static final String QUEUE_NAME = "prefetchdest";

    public static void main(String[] args) throws InterruptedException {
        log.info("Main starts");

        // Send messages to the queue (uncomment if needed)
        // sendMessageBatch();

        // Measure and compare time taken to receive messages with and without prefetching
        long timeWithoutPrefetch = receiveMessages(0);
        long timeWithPrefetch = receiveMessages(500);

        // Calculate and log the time difference
        long timeDifference = timeWithoutPrefetch - timeWithPrefetch;
        log.info("Time difference between prefetch and non-prefetch = {} milliseconds", timeDifference);

        log.info("Main ends");
    }

    /**
     * Creates a list of 1000 messages to be sent to the Service Bus queue.
     *
     * @return a list of ServiceBusMessage objects
     */
    private static List<ServiceBusMessage> createMessages() {
        List<ServiceBusMessage> serviceBusMessages = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            serviceBusMessages.add(new ServiceBusMessage("Message number: " + i));
        }
        return serviceBusMessages;
    }

    /**
     * Sends a batch of messages to the Service Bus queue.
     */
    private static void sendMessageBatch() {
        try (ServiceBusSenderClient senderClient = createSenderClient()) {
            ServiceBusMessageBatch messageBatch = senderClient.createMessageBatch();
            List<ServiceBusMessage> listOfMessages = createMessages();

            for (ServiceBusMessage message : listOfMessages) {
                if (!messageBatch.tryAddMessage(message)) {
                    log.warn("Failed to add message to batch: {}", message.getMessageId());
                }
            }

            senderClient.sendMessages(messageBatch);
            log.info("Message batch sent successfully to the queue: {}", QUEUE_NAME);
        } catch (Exception e) {
            log.error("Error while sending message batch: {}", e.getMessage(), e);
        }
    }

    /**
     * Receives messages from the Service Bus queue with a given prefetch count.
     *
     * @param prefetchCount the number of messages to prefetch
     * @return the time taken to receive messages in milliseconds
     */
    private static long receiveMessages(int prefetchCount) {
        Stopwatch stopWatch = Stopwatch.createStarted();

        try (ServiceBusReceiverClient receiver = createReceiverClient(prefetchCount)) {
            IterableStream<ServiceBusReceivedMessage> messages = receiver.receiveMessages(50, Duration.ofSeconds(5));
            messages.forEach(message -> log.debug("Message received. Id: {}, Contents: {}", message.getMessageId(), message.getBody()));
        } catch (Exception e) {
            log.error("Error while receiving messages: {}", e.getMessage(), e);
        }

        stopWatch.stop();
        long timeTaken = stopWatch.elapsed(TimeUnit.MILLISECONDS);
        log.info("Time to receive and complete all messages with prefetchCount = {}: {} milliseconds", prefetchCount, timeTaken);

        return timeTaken;
    }

    /**
     * Creates a Service Bus sender client for the queue.
     *
     * @return the Service Bus sender client
     */
    private static ServiceBusSenderClient createSenderClient() {
        return new ServiceBusClientBuilder()
                .connectionString(CONNECTION_STRING)
                .sender()
                .queueName(QUEUE_NAME)
                .buildClient();
    }

    /**
     * Creates a Service Bus receiver client with the specified prefetch count.
     *
     * @param prefetchCount the number of messages to prefetch
     * @return the Service Bus receiver client
     */
    private static ServiceBusReceiverClient createReceiverClient(int prefetchCount) {
        return new ServiceBusClientBuilder()
                .connectionString(CONNECTION_STRING)
                .receiver()
                .receiveMode(ServiceBusReceiveMode.PEEK_LOCK)
                .prefetchCount(prefetchCount)
                .queueName(QUEUE_NAME)
                .buildClient();
    }
}
