package mqtt;

import mqtt.utils.TopicQoSInfo;
import mqtt.utils.TopicQoSInfoCollection;
import org.eclipse.paho.client.mqttv3.*;

import java.io.InvalidObjectException;
import java.util.*;

public class MobileClient {
    //region private
    private static final String ServerUri = "tcp://10.0.1.146";//ActiveMQ server uri
    // Quality of Service values:
    // 0 - at most once
    // 1 - at least once
    // 2 - exactly once
    // if QoS is set to -1, subscribe will be skipped
    private static final int QoS = 2;
    //-------------------------------------------------------------------------
    private static final MqttConnectOptions mqttConnectOption = new MqttConnectOptions();
    private static final String ShowSeparator = "--------------------------------------------------------------------------------";
    private static final int DisconnectTimeout = 1000;
    private static final String ClientIdName =  "MobilClientId";
    //-------------------------------------------------------------------------
    private final String clientId;
    private final TopicQoSInfoCollection topicQoSInfoCollection = new TopicQoSInfoCollection();
    //-------------------------------------------------------------------------
    private MqttAsyncClient mqttAsyncClient;
    private MqttClient mqttClient;
    //-------------------------------------------------------------------------
    private Boolean connected = false;
    //-------------------------------------------------------------------------
    private void intConnectAsync() {
        try {

            mqttAsyncClient = new MqttAsyncClient(ServerUri, clientId, null);

            mqttAsyncClient.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable throwable) {
                    clientConnectionLost();
                }

                @Override
                public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
                    showMessage(s, mqttMessage);
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

                }
            });

            mqttAsyncClient.connect(mqttConnectOption, this, new IMqttActionListener() {
                @Override
                public void onSuccess(IMqttToken iMqttToken) {
                    clientConnected(null);
                    subscribeTopics(topicQoSInfoCollection.get());
                }

                @Override
                public void onFailure(IMqttToken iMqttToken, Throwable throwable) {
                    clientConnected(throwable);
                }
            });
        }
        catch (MqttException exception) {
            clientConnected(exception);
        }
    }
    private void intConnect() {
        try {
            mqttClient = new MqttClient(ServerUri, clientId);

            mqttClient.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable throwable) {
                    clientConnectionLost();
                }

                @Override
                public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
                    showMessage(s, mqttMessage);
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

                }
            });

            mqttClient.connect(mqttConnectOption);
            clientConnected(null);
            subscribeTopics(topicQoSInfoCollection.get());
        }
        catch (MqttException exception) {
            clientConnected(exception);
        }
    }

    private void throwableIfConnected() throws InvalidObjectException {
        if(connected) {
            throw new InvalidObjectException("Already connected.");
        }
    }
    //-------------------------------------------------------------------------
    private void clientConnected(Throwable throwable) {
        if(throwable == null) {
            connected = true;
            System.out.printf("%s: Connected (%s: %s).\r\n", new Date(), ClientIdName, clientId);
        }
        else {
            System.out.printf("%s: Connected FAILED (%s: %s).\r\nThrowable: %s.\r\n", new Date(), ClientIdName, clientId, throwable);
        }

    }
    private void clientDisconnected(Throwable throwable) {
        if(throwable == null) {
            connected = false;
            mqttAsyncClient = null;
            mqttClient = null;

            System.out.printf("%s: Disconnected (%s: %s).\r\n", new Date(), ClientIdName, clientId);
        }
        else {
            System.out.printf("%s: Disconnected FAILED (%s: %s).\r\nThrowable: %s.\r\n", new Date(), ClientIdName, clientId, throwable);
        }

    }
    private void clientConnectionLost() {
        connected = false;
        System.out.printf("%s: Connection LOST (%s: ).\r\b", new Date(), ClientIdName, clientId);
    }
    //-------------------------------------------------------------------------
    private void subscribeTopics(TopicQoSInfo[] topicQoSInfos) {
        for (int i = 0; i < topicQoSInfos.length; ++i) {
            TopicQoSInfo topicQoSInfo = topicQoSInfos[i];
            Exception exception = null;
            try {
                if(mqttAsyncClient != null) {
                    mqttAsyncClient.subscribe(topicQoSInfo.getTopicName(), topicQoSInfo.getQoS());
                }
                else {
                    mqttClient.subscribe(topicQoSInfo.getTopicName(), topicQoSInfo.getQoS());
                }
            }
            catch (MqttException exc) {
                exception = exc;
            }
            finally {
                showSubscribeTopic(topicQoSInfo, exception);
            }
        }
    }
    //-------------------------------------------------------------------------
    private void showSubscribeTopic(TopicQoSInfo topicQoSInfo, Throwable throwable) {
        if(throwable == null) {
            System.out.printf("%s: SUBSCRIBE (%s: %s, TopicName: %s, QoS: %s).\r\n", new Date(), ClientIdName, clientId, topicQoSInfo.getTopicName(), topicQoSInfo.getQoSString());
        }
        else {
            System.out.printf("%s: SUBSCRIBE FAILED (%s: %s, TopicName: %s).\r\nThrowable: %s.\r\n", new Date(), ClientIdName, clientId, topicQoSInfo.getTopicName(), throwable);
        }
    }
    private void showUnsubscribeTopic(String topicName, Throwable throwable) {
        if(throwable == null) {
            System.out.printf("%s: UNSUBSCRIBE (%s: %s, Topic: %s).\r\n", new Date(), ClientIdName, clientId, topicName);
        }
        else {
            System.out.printf("%s: UNSUBSCRIBE FAILED (%s: %s, Topic: %s).\r\nThrowable: %s.\r\n", new Date(), ClientIdName, clientId, topicName, throwable);
        }
    }

    private void showMessage(String topicName, MqttMessage message) {
        int messageSize = message.getPayload().length;
        System.out.printf("%s: RECEIVED (%s: %s, Topic: %s, MessageSize: %s).\r\n", new Date(), ClientIdName, clientId, topicName, messageSize);
    }
    private void showSeparator() {
        System.out.println(ShowSeparator);
    }
    //-------------------------------------------------------------------------
    private static int randInt(int min, int max) {
        Random rand = new Random();
        return rand.nextInt((max - min) + 1) + min;
    }
    //endregion
    public MobileClient(String clientId) {
        this(clientId, new String[] { });
    }
    public MobileClient(String clientId, String topicName) {
        this(clientId, new String[] { topicName });
    }
    public MobileClient(String clientId, String[] topicNames) {
        if(clientId == null || clientId.length() == 0) {
            throw new NullPointerException("clientId");
        }

        addTopics(topicNames);

        this.clientId = clientId;

        mqttConnectOption.setConnectionTimeout(1000);
        mqttConnectOption.setCleanSession(false);
    }

    //-------------------------------------------------------------------------
    public boolean isConnected() {
        return connected;
    }
    //-------------------------------------------------------------------------
    public void connect() throws InvalidObjectException {
        throwableIfConnected();
        intConnect();
    }
    public void connect(String topicName) throws InvalidObjectException {
        throwableIfConnected();
        addTopic(topicName);
        intConnect();
    }
    public void connect(String[] topicNames) throws InvalidObjectException {
        throwableIfConnected();
        addTopics(topicNames);
        intConnect();
    }

    public void connectAsync() throws InvalidObjectException {
        throwableIfConnected();
        intConnectAsync();
    }
    public void connectAsync(String topicName) throws InvalidObjectException {
        throwableIfConnected();
        addTopic(topicName);
        intConnectAsync();
    }
    public void connectAsync(String [] topicNames) throws InvalidObjectException {
        throwableIfConnected();
        addTopics(topicNames);
        intConnectAsync();
    }

    public void disconnect() {
        if(connected) {
            try {
                if(mqttAsyncClient != null) {
                    mqttAsyncClient.disconnect(DisconnectTimeout, this, new IMqttActionListener() {
                        @Override
                        public void onSuccess(IMqttToken iMqttToken) {
                            clientDisconnected(null);
                        }

                        @Override
                        public void onFailure(IMqttToken iMqttToken, Throwable throwable) {
                            clientDisconnected(throwable);
                        }
                    });
                }
                else {
                    mqttClient.disconnect(DisconnectTimeout);
                    clientDisconnected(null);
                }
            }
            catch (MqttException exception) {
                clientDisconnected(exception);
            }
        }
    }
    //-------------------------------------------------------------------------
    public void addTopic(String topicName) {
        topicQoSInfoCollection.add(topicName, QoS);

        if(connected) {
            TopicQoSInfo topicQoSInfo = topicQoSInfoCollection.get(topicName);
            Throwable throwable = null;
            try {
                if(mqttAsyncClient != null) {
                    mqttAsyncClient.subscribe(topicQoSInfo.getTopicName(), topicQoSInfo.getQoS());
                }
                else {
                    mqttClient.subscribe(topicQoSInfo.getTopicName(), topicQoSInfo.getQoS());
                }
            }
            catch (MqttException exc) {
                throwable = exc;
            }
            finally {
                showSubscribeTopic(topicQoSInfo, throwable);
            }
        }
    }
    public void addTopics(String[] topicNames) {
        topicQoSInfoCollection.add(topicNames, QoS);

        if(connected) {
            TopicQoSInfo[] topicQoSInfos = topicQoSInfoCollection.get(topicNames);
            subscribeTopics(topicQoSInfos);
        }
    }
    //-------------------------------------------------------------------------
    public void removeTopic(String topicName) {
        topicQoSInfoCollection.remove(topicName);

        if(connected) {
            Throwable throwable = null;
            try {
                if(mqttAsyncClient != null) {
                    mqttAsyncClient.unsubscribe(topicName);
                }
                else {
                    mqttClient.unsubscribe(topicName);
                }
            }
            catch(MqttException exc) {
                throwable = exc;
            }
            finally {
                showUnsubscribeTopic(topicName, throwable);
            }
        }
    }
    public void removeTopics(String[] topicNames) {
        String[] removedTopicNames = topicQoSInfoCollection.remove(topicNames);

        if(connected) {
            for (int i = 0; i < topicNames.length; ++i) {
                String topicName = topicNames[i];
                Throwable throwable = null;
                try {
                    if(mqttAsyncClient != null) {
                        mqttAsyncClient.unsubscribe(topicName);
                    }
                    else {
                        mqttClient.unsubscribe(topicName);
                    }
                }
                catch(MqttException exc) {
                    throwable = exc;
                }
                finally {
                    showUnsubscribeTopic(topicName, throwable);
                }
            }
        }
    }
    //-------------------------------------------------------------------------
    public String getClientId() {
        return clientId;
    }
    public String[] getTopicNames() {
        return topicQoSInfoCollection.getTopicNames();
    }
    //-------------------------------------------------------------------------
}
