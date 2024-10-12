package com.luna.app.topic;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import io.github.cdimascio.dotenv.Dotenv;

public class ASBTopicReceiverA {

    //    private static final Logger logger = LoggerFactory.getLogger(ASBTopicReceiverA.class);
    private static final Dotenv dotenv = Dotenv.load();

    private static final String CONNECTION_STRING = dotenv.get("AZURE_SERVICE_BUS_CONNECTION_STRING");
    private static final String TOPIC_NAME = dotenv.get("AZURE_SERVICE_BUS_TOPIC_NAME");
    private static final String SUBSCRIPTION_NAME = dotenv.get("AZURE_SERVICE_BUS_SUBSCRIPTION_A");  // Subscription A

    public static void main(String[] args) {
        ASBTopicReceiverA receiver = new ASBTopicReceiverA();
        receiver.receiveMessages();
    }

    /**
     * Receives messages from the Service Bus topic subscription.
     */
    public void receiveMessages() {
        try (ServiceBusReceiverClient receiverClient = createReceiverClient()) {
            System.out.println("Listening for messages from Subscription A...");
            while (true) {
                receiverClient.receiveMessages(1).forEach(message -> {
                    processMessage(receiverClient, message);
                });
                Thread.sleep(1000); // Polling interval
            }
        } catch (Exception e) {
            System.out.println("Error occurred while receiving messages: {}" + e.getMessage());
        }
    }


    private ServiceBusReceiverClient createReceiverClient() {
        return new ServiceBusClientBuilder()
                .connectionString(CONNECTION_STRING)
                .receiver()
                .topicName(TOPIC_NAME)
                .subscriptionName(SUBSCRIPTION_NAME)
                .buildClient();
    }


    private void processMessage(ServiceBusReceiverClient receiverClient, ServiceBusReceivedMessage message) {
        try {
            System.out.println("Subscription A - Message received: {}" + message.getBody().toString());
            receiverClient.complete(message);
        } catch (Exception e) {
            System.out.println("Error processing message: {}" + e.getMessage());
        }
    }
}
