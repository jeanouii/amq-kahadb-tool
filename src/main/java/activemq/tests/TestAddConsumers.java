package activemq.tests;

import activemq.clients.Consumer;

import static activemq.Utils.showException;

public class TestAddConsumers {
    //region private
    private static final int ConsumerCount = 3;
    //-------------------------------------------------------------------------
    private static final String ClientName = "client";
    private static final String TopicName = "test.topic.";
    //endregion
    public static void main(String[] args) {
        try {
            Consumer consumer = new Consumer();

            for (int i = 0; i < ConsumerCount; ++i) {
                consumer.connect(ClientName + i);
                consumer.subscribe(TopicName + i, ClientName + i + "_" + TopicName + i, false);
                consumer.disconnect();
            }
        }
        catch (Throwable throwable) {
            showException(throwable);
        }
    }
}
