package com.luna.app.queue;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import io.github.cdimascio.dotenv.Dotenv;

public class ASBQueueSender {
    private static final Dotenv dotenv = Dotenv.load();

    private static final String CONNECTION_STRING = dotenv.get("AZURE_SERVICE_BUS_CONNECTION_STRING");
    private static final String QUEUE_NAME = dotenv.get("AZURE_SERVICE_BUS_QUEUE_NAME");


    public static void main(String[] args) {
        String message = "Hello World 3";
        sendMessageToAzureServiceBusQueue(message);
    }

    public static void sendMessageToAzureServiceBusQueue(String message) {
        try (ServiceBusSenderClient senderClient = new ServiceBusClientBuilder()
                .connectionString(CONNECTION_STRING)
                .sender()
                .queueName(QUEUE_NAME)
                .buildClient()) {

            ServiceBusMessage serviceBusMessage = new ServiceBusMessage(message);
            senderClient.sendMessage(serviceBusMessage);
            System.out.println("Message sent to queue successfully.");

        } catch (Exception e) {
            System.out.println("Error while sending message to the queue: {}" + e.getMessage());
        }
    }
}
