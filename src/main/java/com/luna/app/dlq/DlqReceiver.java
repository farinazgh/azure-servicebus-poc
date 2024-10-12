package com.luna.app.dlq;

import com.azure.core.util.IterableStream;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;

import java.util.List;

public class DlqReceiver {

    //    private static final Logger logger = LoggerFactory.getLogger(DlqReceiverExplicitExample.class);
    private static final String CONNECTION_STRING = System.getenv("AZURE_SERVICE_BUS_CONNECTION_STRING");
    private static final String QUEUE_NAME = System.getenv("AZURE_SERVICE_BUS_QUEUE_NAME");
    private static final List<String> MOONS = List.of("Io", "Europa", "Ganymede", "Callisto", "Himalia");

    public static void main(String[] args) {
        DlqReceiver receiver = new DlqReceiver();
        receiver.receiveMessages();
    }


    public void receiveMessages() {
        try (ServiceBusReceiverClient receiverClient = createReceiverClient()) {
            System.out.println("Receiving messages from queue: {}" + QUEUE_NAME);

            while (true) {
                IterableStream<ServiceBusReceivedMessage> messages = receiverClient.receiveMessages(1);
                messages.forEach(message -> {
                    processMessage(receiverClient, message);
                });
                Thread.sleep(1000);
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Polling interrupted: {}" + e.getMessage());
        } catch (Exception e) {
            System.err.println("Error receiving messages: {}" + e.getMessage());
        }
    }


    private ServiceBusReceiverClient createReceiverClient() {
        return new ServiceBusClientBuilder()
                .connectionString(CONNECTION_STRING)
                .receiver()
                .queueName(QUEUE_NAME)
                .buildClient();
    }


    private void processMessage(ServiceBusReceiverClient receiverClient, ServiceBusReceivedMessage message) {
        String messageBody = message.getBody().toString();

        if (MOONS.contains(messageBody)) {
            System.out.println("Valid message: {}. Completing the message." + messageBody);
            receiverClient.complete(message);
        } else {
            System.out.println("Invalid message: {}. Dead-lettering the message." + messageBody);
            //main story happens here
            receiverClient.deadLetter(message);
        }
    }
}
