package com.luna.app.session;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusReceiverAsyncClient;
import com.azure.messaging.servicebus.ServiceBusSessionReceiverAsyncClient;
import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Demonstrates how to receive from the first available session.
 */
public class ReceiveSingleSessionAsyncExample {

    private static final Logger logger = LoggerFactory.getLogger(ReceiveSingleSessionAsyncExample.class);

    // Use environment variables for sensitive information
    private static final String CONNECTION_STRING = System.getenv("AZURE_SERVICE_BUS_CONNECTION_STRING");
    private static final String QUEUE_NAME = "queue001";

    public static void main(String[] args) throws InterruptedException {
        receiveSingleSession();
    }

    /**
     * Receives messages from the first available session in an asynchronous manner.
     */
    private static void receiveSingleSession() throws InterruptedException {
        AtomicBoolean operationSuccessful = new AtomicBoolean(true);
        CountDownLatch countdownLatch = new CountDownLatch(1);

        // Create a session receiver client
        try (ServiceBusSessionReceiverAsyncClient sessionReceiver = new ServiceBusClientBuilder()
                .connectionString(CONNECTION_STRING)
                .sessionReceiver()
                .receiveMode(ServiceBusReceiveMode.PEEK_LOCK)
                .queueName(QUEUE_NAME)
                .buildAsyncClient()) {

            // Accept the next available session
            Mono<ServiceBusReceiverAsyncClient> receiverMono = sessionReceiver.acceptNextSession();

            // Process the session messages
            Disposable subscription = Flux.usingWhen(
                    receiverMono,
                    receiver -> receiver.receiveMessages(),
                    receiver -> Mono.fromRunnable(receiver::close)
            ).subscribe(message -> {
                        logger.info("Session: {}. Sequence #: {}. Contents: {}",
                                message.getSessionId(), message.getSequenceNumber(), message.getBody());
                    },
                    error -> {
                        logger.error("Error occurred while receiving messages: {}", error.getMessage());
                        operationSuccessful.set(false);
                    });

            // Wait for the receiving operation to complete
            countdownLatch.await(30, TimeUnit.SECONDS);

            // Dispose of the subscription and close the session receiver
            subscription.dispose();
        } catch (Exception e) {
            logger.error("Error occurred while closing the session receiver: {}", e.getMessage(), e);
            operationSuccessful.set(false);
        }

        logger.info("Message receiving process completed successfully: {}", operationSuccessful.get());
    }
}
