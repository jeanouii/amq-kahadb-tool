package org.hill30.activemq.examples;

import org.hill30.activemq.clients.MqttConsumer;

import static org.hill30.activemq.Utils.showException;

public class ExampleMqttConsumerReceiveMessage {
    //region private
    private static final int ConsumerCount = 1;
    //-------------------------------------------------------------------------
    private static final String ClientIdFixedName = "client";
    private static final String TopicFixedName = "test/topic";
    private static final int QoS = 2;
    //endregion
    public static void main(String[] args) {
        for (int i = 0; i < ConsumerCount; ++i) {
            String clientId = String.format("%s.%s", ClientIdFixedName, i);
            String topicName = String.format("%s/%s", TopicFixedName, i);

            MqttConsumer consumer = new MqttConsumer(new MqttConsumer.IMessageAdapter() {
                @Override
                public void showMessage(String message) {
                    System.out.println(message);
                }
                @Override
                public void showMessage(MqttConsumer.MessageInfo message) {
                    message.show();
                }
            });

            try {
                consumer.connect(clientId);
                if(consumer.isConnected()) {
                    consumer.addTopicAfterConnect(topicName, QoS);
                    Thread.sleep(1000);
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
