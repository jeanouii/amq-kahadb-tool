package activemq.tests;

import mqtt.MobileClient;

import java.io.FileNotFoundException;

public class AddSubscriptions {
    //region private
    private static final int SubscriptionCount = 100;
    private static final String ClientName = "Client";
    private static final String TopicName = "Topic";
    //endregion
    public static void main(String[] args) throws FileNotFoundException {
        try {
            for (int i = 0; i < SubscriptionCount; ++i) {

                MobileClient client = new MobileClient(ClientName + i, TopicName + i);
                client.connect();
                Thread.sleep(50);
                client.disconnect();
            }
        }
        catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }
}
