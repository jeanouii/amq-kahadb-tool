package activemq.clients;

import org.apache.activemq.store.kahadb.data.KahaDestination;
import org.eclipse.paho.client.mqttv3.*;

import javax.jms.JMSException;
import java.io.InvalidObjectException;
import java.util.Date;

import static activemq.Utils.isNullOrEmpty;
import static activemq.Utils.showException;

public class MqttConsumer {
    //region private
    private static final String ClientIdName =  "MqqtClientId";
    private static final String DestinationType = KahaDestination.DestinationType.TOPIC.toString();
    //-------------------------------------------------------------------------
    private MqttClient mqttClient;
    private String clientId;
    //-------------------------------------------------------------------------
    private void clientConnected(Throwable throwable, String clientId) {
        if(throwable == null) {
            this.clientId = clientId;
            System.out.printf("%s: Connected (%s: %s).\r\n", new Date(), ClientIdName, clientId);
        }
        else {
            System.out.printf("%s: Connected FAILED (%s: %s).\r\nThrowable: %s.\r\n", new Date(), ClientIdName, clientId, throwable);
        }

    }
    private void clientDisconnected(Throwable throwable) {
        if(throwable == null) {
            System.out.printf("%s: Disconnected (%s: %s).\r\n", new Date(), ClientIdName, clientId);
            clientId = null;
        }
        else {
            System.out.printf("%s: Disconnected FAILED (%s: %s).\r\nThrowable: %s.\r\n", new Date(), ClientIdName, clientId, throwable);
        }
    }
    private void clientConnectionLost() {
        System.out.printf("%s: Connection LOST (%s: ).\r\b", new Date(), ClientIdName, clientId);
        clientId = null;
    }
    //-------------------------------------------------------------------------
    private void showSubscribeDestination(String destinationName, int qos) throws JMSException {
        String destinationType = KahaDestination.DestinationType.TOPIC.toString();
        System.out.printf("%s: +SUBSCRIBE (%s: %s, %s: %s, QoS: %s).\r\n", new Date(), ClientIdName, clientId, DestinationType, destinationName, qoSToString(qos));
    }
    private void showUnsubscribeDestination(String destinationName) {
        System.out.printf("%s: -SUBSCRIBE (%s: %s, %s: %s).\r\n", new Date(), ClientIdName, clientId, DestinationType, destinationName);
    }
    private void showReceiveMessage(String destinationName, MqttMessage message) {
        int messageSize = message.getPayload().length;
        System.out.printf("%s: RECEIVED (%s: %s, %s: %s, MessageSize: %s).\r\n", new Date(), ClientIdName, clientId, DestinationType, destinationName, messageSize);
    }
    //-------------------------------------------------------------------------
    private String qoSToString(int qos) {
        switch (qos) {
            case 0: {
                return "AT_MOST_ONCE";
            }
            case 1: {
                return "AT_LEAST_ONCE";
            }
            case 2: {
                return "EXACTLY_ONCE";
            }
            default: {
                return "-";
            }
        }
    }
    //endregion
    //-------------------------------------------------------------------------
    public boolean isConnected() {
        return clientId != null;
    }
    //-------------------------------------------------------------------------
    public void connect(String clientId) throws InvalidObjectException {
        connect(clientId, "tcp://10.0.1.146");
    }
    public void connect(String clientId, String serverUri) throws InvalidObjectException {
        if(isNullOrEmpty(clientId)) {
            throw new NullPointerException("clientId");
        }
        if(isNullOrEmpty(serverUri)) {
            throw new NullPointerException("serverUri");
        }
        if(isConnected()) {
            throw new InvalidObjectException("Already connected");
        }

        Throwable throwable = null;
        try {
            mqttClient = new MqttClient(serverUri, clientId);
            mqttClient.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable throwable) {
                    clientConnectionLost();
                }

                @Override
                public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
                    showReceiveMessage(s, mqttMessage);
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

                }
            });

            MqttConnectOptions mqttConnectOption = new MqttConnectOptions();
            mqttConnectOption.setConnectionTimeout(1000);
            mqttConnectOption.setCleanSession(false);
            mqttClient.connect(mqttConnectOption);
        }
        catch (Throwable e) {
            throwable = e;
        }
        finally {
            clientConnected(throwable, clientId);
        }
    }

    public void disconnect() {
        if(isConnected()) {
            Throwable throwable = null;
            try {
                mqttClient.disconnect(1000);
            }
            catch (Throwable e) {
                throwable = e;
            }
            finally {
                clientDisconnected(throwable);
            }
        }
    }
    //-------------------------------------------------------------------------
    public void subscribe(String topicName, int qos) throws InvalidObjectException {
        if(isNullOrEmpty(topicName)) {
            throw new NullPointerException("topicName");
        }
        if(!isConnected()) {
            throw new InvalidObjectException("You must call the function connect().");
        }

        try {
            mqttClient.subscribe(topicName, qos);
            showSubscribeDestination(topicName, qos);
        }
        catch (Throwable throwable) {
            showException(throwable);
        }
    }
    public void unsubscribe(String topicName) throws InvalidObjectException {
        if(isNullOrEmpty(topicName)) {
            throw new NullPointerException("topicName");
        }
        if(!isConnected()) {
            throw new InvalidObjectException("You must call the function connect().");
        }

        try {
            mqttClient.unsubscribe(topicName);
            showUnsubscribeDestination(topicName);
        }
        catch (Throwable throwable) {
            showException(throwable);
        }
    }
    //-------------------------------------------------------------------------
}
