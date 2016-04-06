package activemq.tests;

import activemq.clients.MqttConsumer;

import static activemq.Utils.showException;

public class TestAddMqttConsumers {
    //region private
    private static final int ConsumerCount = 2;
    //-------------------------------------------------------------------------
    private static final String ClientName = "client";
    private static final String TopicName = "test/topic/";
    //endregion
    public static void main(String[] args) {
        try {
            MqttConsumer consumer = new MqttConsumer();

            for (int i = 0; i < ConsumerCount; ++i) {
                consumer.connect(ClientName + i);
                consumer.subscribe(TopicName + i, 2);
                Thread.sleep(1000);
                consumer.disconnect();
            }
        }
        catch (Throwable throwable) {
            showException(throwable);
        }
    }
}
