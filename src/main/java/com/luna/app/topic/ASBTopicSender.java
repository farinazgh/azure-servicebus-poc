package com.luna.app.topic;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.azure.messaging.servicebus.ServiceBusMessage;
import io.github.cdimascio.dotenv.Dotenv;

public class ASBTopicSender {

    //    private static final Logger logger = LoggerFactory.getLogger(ASBTopicSender.class);
    private static final Dotenv dotenv = Dotenv.load();
    private static final String CONNECTION_STRING = dotenv.get("AZURE_SERVICE_BUS_CONNECTION_STRING");
    private static final String TOPIC_NAME = dotenv.get("AZURE_SERVICE_BUS_TOPIC_NAME");

    public static void main(String[] args) {
        ASBTopicSender sender = new ASBTopicSender();
        sender.sendMessage("Simple message to carme.");
    }


    public void sendMessage(String message) {
        try (ServiceBusSenderClient senderClient = createSenderClient()) {
            sendMessageToTopic(senderClient, message);
        } catch (Exception e) {
            System.out.println("Error occurred while sending message to topic: {} " + e.getMessage());
        }
    }


    private ServiceBusSenderClient createSenderClient() {
        return new ServiceBusClientBuilder()
                .connectionString(CONNECTION_STRING)
                .sender()
                .topicName(TOPIC_NAME)
                .buildClient();
    }


    private void sendMessageToTopic(ServiceBusSenderClient senderClient, String message) {
        try {
            ServiceBusMessage serviceBusMessage = new ServiceBusMessage(message);
            senderClient.sendMessage(serviceBusMessage);
            System.out.println("Message sent to topic: {} " + TOPIC_NAME);
        } catch (Exception e) {
            System.out.println("Failed to send message to topic: {} " + e.getMessage());
        }
    }
}
