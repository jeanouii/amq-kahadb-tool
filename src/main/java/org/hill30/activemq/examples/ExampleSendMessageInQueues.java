package org.hill30.activemq.examples;

import org.hill30.activemq.clients.Producer;

import static org.hill30.activemq.Utils.*;

public class ExampleSendMessageInQueues {
    //region private
    private static final int QueueCount = 1;
    //-------------------------------------------------------------------------
    private static final String Message = "Enter some text here for the message body...";
    //-------------------------------------------------------------------------
    private static final String ProducerClietIdFixedName = "proClient";
    private static final String QueueFixedName = "test.queue";
    //endregion
    public static void main(String[] args) {
        String clientId = String.format("%s.%s", ProducerClietIdFixedName, 1);
        Producer producer = new Producer(System.out::println);

        try {
            producer.connect(clientId);

            if(producer.isConnected()) {
                for (int i = 0; i < QueueCount; ++i) {
                    String queueName = String.format("%s.%s", QueueFixedName, i);
                    producer.sendMessageInQueue(Message, queueName);
                }
            }
        }
        catch (Throwable throwable) {
            showException(throwable);
        }
        finally {
            producer.disconnect();
        }
    }
}
