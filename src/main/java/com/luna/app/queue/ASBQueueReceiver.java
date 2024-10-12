package com.luna.app.queue;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import io.github.cdimascio.dotenv.Dotenv;

public class ASBQueueReceiver {

    //    private static final Logger logger = LoggerFactory.getLogger(ASBQueueReceiver.class);
    private static final Dotenv dotenv = Dotenv.load();

    private static final String CONNECTION_STRING = dotenv.get("AZURE_SERVICE_BUS_CONNECTION_STRING");
    private static final String QUEUE_NAME = dotenv.get("AZURE_SERVICE_BUS_QUEUE_NAME");
    // Polling constants
    private static final int MESSAGE_BATCH_SIZE = 1;
    private static final long POLLING_INTERVAL_MS = 1000L; // 1 second

    public static void main(String[] args) {
        ASBQueueReceiver receiver = new ASBQueueReceiver();
        receiver.startMessageReceiver();
    }


    public void startMessageReceiver() {
        try (ServiceBusReceiverClient receiverClient = createReceiverClient()) {
            System.out.println("Listening for messages from the queue: {} " + QUEUE_NAME);
            pollMessages(receiverClient);
        } catch (Exception e) {
            System.out.println("Error while receiving messages: {} " + e.getMessage());
        }
    }

    /**
     * Polls the queue for messages indefinitely.
     *
     * @param receiverClient the Service Bus receiver client
     * @throws InterruptedException if the thread is interrupted while sleeping
     */
    private void pollMessages(ServiceBusReceiverClient receiverClient) throws InterruptedException {
        while (true) {//todo better way than while loop
            receiverClient.receiveMessages(MESSAGE_BATCH_SIZE).forEach(message -> {
                processMessage(receiverClient, message);
            });
            Thread.sleep(POLLING_INTERVAL_MS); // Poll every second
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
        try {
            System.out.println("Message received from queue: {}" + message.getBody());
            receiverClient.complete(message);  // Mark message as complete (it seems it deletes the message)
            System.out.println("Message processed successfully.");
        } catch (Exception e) {
            System.out.println("Error processing message: {} " + e.getMessage());
        }
    }
}
