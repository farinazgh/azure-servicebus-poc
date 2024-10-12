package com.luna.app.dlq;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;

import java.time.Duration;
import java.time.Instant;

public class DLQMaxDeliveryCount {

//    private static final Logger logger = LoggerFactory.getLogger(DLQMaxDeliveryCount.class);

    private static final String CONNECTION_STRING = System.getenv("AZURE_SERVICE_BUS_CONNECTION_STRING");
    private static final String QUEUE_NAME = System.getenv("AZURE_SERVICE_BUS_QUEUE_NAME");
    // Set the duration for receiving messages (60 seconds)
    private static final Duration RECEIVE_DURATION = Duration.ofSeconds(60);

    public static void main(String[] args) {
        DLQMaxDeliveryCount receiver = new DLQMaxDeliveryCount();
        receiver.receiveMessages();
    }

    /**
     * Receives messages from the Azure Service Bus queue for a specified duration and abandons them.
     */
    public void receiveMessages() {
        try (ServiceBusReceiverClient receiverClient = createReceiverClient()) {
            Instant startTime = Instant.now();
            System.out.println("Receiving messages from queue: {}" + QUEUE_NAME);

            while (Instant.now().isBefore(startTime.plus(RECEIVE_DURATION))) {
                receiverClient.receiveMessages(1).forEach(message -> {
                    System.out.println("Message received: {}" + message.getBody());
                    abandonMessage(receiverClient, message);
                });
                Thread.sleep(1000);  // Poll every second
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Message receiving interrupted: {}" + e.getMessage());
        } catch (Exception e) {
            System.out.println("Error occurred while receiving messages: {}" + e.getMessage());
        }
    }


    private ServiceBusReceiverClient createReceiverClient() {
        return new ServiceBusClientBuilder()
                .connectionString(CONNECTION_STRING)
                .receiver()
                .queueName(QUEUE_NAME)
                .buildClient();
    }


    private void abandonMessage(ServiceBusReceiverClient receiverClient, com.azure.messaging.servicebus.ServiceBusReceivedMessage message) {
        try {
            receiverClient.abandon(message);
            System.out.println("Message abandoned: {}" + message.getBody());
        } catch (Exception e) {
            System.out.println("Error abandoning message: {}" + e.getMessage());
        }
    }
}
