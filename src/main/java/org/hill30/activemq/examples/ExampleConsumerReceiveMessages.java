package org.hill30.activemq.examples;

import org.hill30.activemq.clients.Consumer;

import static org.hill30.activemq.Utils.*;

public class ExampleConsumerReceiveMessages {
    //region private
    private static final int ConsumerCount = 1;
    //-------------------------------------------------------------------------
    private static final String ClientIdFixedName = "client";
    private static final String TopicFixedName = "test.topic";
    private static final String QueueFixedName = "test.queue";
    //endregion
    public static void main(String[] args) {
        for (int i = 0; i < ConsumerCount; ++i) {
            String clientId = String.format("%s.%s", ClientIdFixedName, i);
            String topicName = String.format("%s.%s", TopicFixedName, i);
            String queueName = String.format("%s.%s", QueueFixedName, i);
            String subscriptionName = String.format("%s_%s", clientId, topicName);

            Consumer consumer = new Consumer(System.out::println);

            try {
                consumer.addTopicBeforeConnect(topicName, subscriptionName);
                consumer.addQueueBeforeConnect(queueName);

                consumer.connect(clientId);

                if (consumer.isConnected()) {
                    Consumer.MessageInfo message;
                    while ((message = consumer.getMessage()) != null) {
                        message.show();
                    }
                }
            }
            catch (Throwable throwable) {
                showException(throwable);
            }
            finally {
                consumer.disconnect();
            }
        }
    }
}
