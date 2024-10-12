
package com.luna.app.miscelanous;

import com.azure.core.util.IterableStream;
import com.azure.messaging.servicebus.*;
import com.azure.messaging.servicebus.administration.ServiceBusAdministrationClient;
import com.azure.messaging.servicebus.administration.ServiceBusAdministrationClientBuilder;
import com.azure.messaging.servicebus.administration.models.QueueProperties;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class AdministrationClientUpdateQueueExample {
    static String connectionString = "";
    static String queueName = "queue001";

    public static void main(String[] args) {
        ServiceBusAdministrationClient adminClient = new ServiceBusAdministrationClientBuilder()
                .connectionString(connectionString)
                .buildClient();
        // Delete the queue if it exists
        deleteQueueIfExists(adminClient, queueName);
        // Create the queue
        QueueProperties queueProperties = createQueue(adminClient, queueName);
        // Update the queue properties
        updateQueueProperties(adminClient, queueProperties);
        // send multiple messages to queue
        sendMessagesToQueue(connectionString, queueName);
        receiveMessagesFromQueue(connectionString, queueName);
        deleteQueueIfExists(adminClient, queueName);
        System.exit(0);
    }

    private static void deleteQueueIfExists(ServiceBusAdministrationClient adminClient, String queueName) {
        if (adminClient.getQueueExists(queueName)) {
            adminClient.deleteQueue(queueName);
            System.out.printf("Queue '%s' deleted.%n", queueName);
        } else {
            System.out.printf("Queue '%s' does not exist.%n", queueName);
        }
    }

    private static QueueProperties createQueue(ServiceBusAdministrationClient adminClient, String queueName) {
        QueueProperties createdQueueProperties = adminClient.createQueue(queueName);
        System.out.println("Queue created: " + createdQueueProperties.getName());
        return createdQueueProperties;
    }

    private static void updateQueueProperties(ServiceBusAdministrationClient adminClient, QueueProperties queueProperties) {
        queueProperties.setMaxDeliveryCount(4)
                .setLockDuration(Duration.ofSeconds(60));
        QueueProperties updatedQueueProperties = adminClient.updateQueue(queueProperties);
        System.out.println("Queue updated: " + updatedQueueProperties.getName());
    }

    private static void sendMessagesToQueue(String connectionString, String queueName) {
        ServiceBusSenderClient senderClient = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .sender()
                .queueName(queueName)
                .buildClient();

        List<ServiceBusMessage> messages = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            messages.add(new ServiceBusMessage("Message " + i));
        }
        senderClient.sendMessages(messages);
        System.out.println("Sent messages to queue: " + queueName);
    }

    private static void receiveMessagesFromQueue(String connectionString, String queueName) {
        ServiceBusReceiverClient receiverClient = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .receiver()
                .queueName(queueName)
                .buildClient();
        IterableStream<ServiceBusReceivedMessage> messages = receiverClient.receiveMessages(10, Duration.ofSeconds(10));
        for (ServiceBusReceivedMessage message : messages) {
            System.out.printf("Received message with sequence number '%s': %s%n",
                    message.getSequenceNumber(), message.getBody().toString());
            // receiverClient.complete(message) is a method that completes the receive operation of a
            // message and indicates that the message should be marked as processed and deleted.
            // It requires the lock token of the message as a parameter.
            // This method is used with Microsoft.Azure.ServiceBus.Core.Message class.
            receiverClient.complete(message);
        }
        System.out.println("received all messages");
        receiverClient.close();
    }
}
