package org.hill30.activemq.examples;

import org.hill30.activemq.clients.Producer;

import static org.hill30.activemq.Utils.showException;

public class ExampleSendMessageInTopics {
    //region private
    private static final int TopicCount = 1;
    //-------------------------------------------------------------------------
    private static final String Message = "Enter some text here for the message body...";
    //-------------------------------------------------------------------------
    private static final String ProducerClietIdFixedName = "proClient";
    private static final String TopicFixedName = "test.topic";
    //endregion
    public static void main(String[] args) {
        String clientId = String.format("%s.%s", ProducerClietIdFixedName, 1);
        Producer producer = new Producer(System.out::println);

        try {
            producer.connect(clientId);

            if(producer.isConnected()) {
                for (int i = 0; i < TopicCount; ++i) {
                    String topicName = String.format("%s.%s", TopicFixedName, i);
                    producer.sendMessageInTopic(Message, topicName);
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
