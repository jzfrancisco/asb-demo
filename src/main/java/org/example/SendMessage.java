package org.example;

import com.azure.messaging.servicebus.*;

import java.util.Arrays;
import java.util.List;

public class SendMessage {
    static String connectionString = "Endpoint=sb://jc-queue-example.servicebus.windows.net/;SharedAccessKeyName=policy-sample-queue;SharedAccessKey=IVKbkp9uX4XCfPS7UVdCUZX+oFp8CCGl8+ASbJqMemI=;EntityPath=sample-queue";
    static String queueName = "sample-queue";

    public static void main(String[] args) {
//        sendMessage();
        sendMessageBatch();
    }

    static void sendMessage()  {
        // create a Service Bus Sender client for the queue
        ServiceBusSenderClient senderClient = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .sender()
                .queueName(queueName)
                .buildClient();

        // send one message to the queue
        senderClient.sendMessage(new ServiceBusMessage("Hello, World!"));
        System.out.println("Sent a single message to the queue: " + queueName);
    }

    static List<ServiceBusMessage> createMessages() {
        ServiceBusMessage[] messages = {
                new ServiceBusMessage("First message"),
                new ServiceBusMessage("Second message"),
                new ServiceBusMessage("Third message")
        };
        return Arrays.asList(messages);
    }

    static void sendMessageBatch() {
        ServiceBusSenderClient senderClient = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .sender()
                .queueName(queueName)
                .buildClient();

        ServiceBusMessageBatch messageBatch = senderClient.createMessageBatch();

        List<ServiceBusMessage> listOfMessages = createMessages();

        for (ServiceBusMessage message : listOfMessages) {
            if (messageBatch.tryAddMessage(message)) {
                continue;
            }

            // The batch is full, so we create a new batch and send the batch.
            senderClient.sendMessages(messageBatch);
            System.out.println("Sent a batch of messages to the queue: " + queueName);

            // create a new batch
            messageBatch = senderClient.createMessageBatch();

            // Add that message that we couldn't before.
            if (!messageBatch.tryAddMessage(message)) {
                System.err.printf("Message is too large for an empty batch. Skipping. Max size: %s.", messageBatch.getMaxSizeInBytes());
            }
        }

        if (messageBatch.getCount() > 0) {
            senderClient.sendMessages(messageBatch);
            System.out.println("Sent a batch of messages to the queue: " + queueName);
        }

        //close the client
        senderClient.close();
    }
}
