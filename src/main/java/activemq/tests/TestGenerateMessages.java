package activemq.tests;

import activemq.clients.Producer;

import static activemq.Utils.showException;

public class TestGenerateMessages {
    //region private
    private static final int TopicCount = 3;
    //-------------------------------------------------------------------------
    private static final String Message = "Test message";
    //-------------------------------------------------------------------------
    private static final String ClientName = "proClient";
    private static final String TopicName = "test.topic.";
    //endregion
    public static void main(String[] args) {
        try {
            Producer producer = new Producer();
            producer.connect(ClientName);

            for (int i = TopicCount - 1; i >= 0; --i) {
                producer.sendMessageInTopic(Message, TopicName + i, true);
            }

            producer.disconnect();
        }
        catch (Throwable throwable) {
            showException(throwable);
        }
    }
}
