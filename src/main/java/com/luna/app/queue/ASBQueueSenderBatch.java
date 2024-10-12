package com.luna.app.queue;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import io.github.cdimascio.dotenv.Dotenv;

public class ASBQueueSenderBatch {

    private static final Dotenv dotenv = Dotenv.load();

    private static final String CONNECTION_STRING = dotenv.get("AZURE_SERVICE_BUS_CONNECTION_STRING");
    private static final String QUEUE_NAME = dotenv.get("AZURE_SERVICE_BUS_QUEUE_NAME");

    public static void main(String[] args) {
        sendMessageToAzureServiceBusQueue("Hello World");
    }

    public static void sendMessageToAzureServiceBusQueue(String message) {
        try (ServiceBusSenderClient senderClient = new ServiceBusClientBuilder()
                .connectionString(CONNECTION_STRING)
                .sender()
                .queueName(QUEUE_NAME)
                .buildClient()) {

            senderClient.sendMessage(new ServiceBusMessage(message));
            System.out.println("Message sent to queue successfully.");

        } catch (Exception e) {
            System.err.println("Error while sending message: " + e.getMessage());
        }
    }
}
