package org.hill30.activemq.clients;

import org.apache.activemq.store.kahadb.data.KahaDestination;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import javax.jms.InvalidDestinationException;
import java.io.InvalidObjectException;
import java.net.InetAddress;
import java.util.Date;

import static org.hill30.activemq.Utils.isNullOrEmpty;
import static org.hill30.activemq.Utils.showException;

public class MqttConsumer {
    //region private
    private static final String ServerUriFixedName = "SrvUri";
    private static final String ClientIdFixedName =  "MqttClntId";
    //-------------------------------------------------------------------------
    private final IMessageAdapter messageAdapter;
    //-------------------------------------------------------------------------
    private MqttClient mqttClient;
    private String serverUri;
    private String clientId;
    //-------------------------------------------------------------------------
    private void showClientConnected(String serverUri, String clientId, Throwable throwable) {
        String failedTitle = throwable == null ? "" : "FAILED ";
        String throwableMessage = throwable == null ? "" : String.format("\r\nThrowable: %s.", throwable);
        String message = String.format("%s: Connected %s(%s: %s, %s: %s).%s",
                new Date(),
                failedTitle,
                ServerUriFixedName,
                serverUri,
                ClientIdFixedName,
                clientId,
                throwableMessage
        );
        messageAdapter.showMessage(message);
    }
    private void showClientDisconnected(String serverUri, String clientId, Throwable throwable) {
        String failedTitle = throwable == null ? "" : "FAILED ";
        String throwableMessage = throwable == null ? "" : String.format("\r\nThrowable: %s", throwable);
        String message = String.format("%s: Disconnected %s(%s: %s, %s: %s).%s",
                new Date(),
                failedTitle,
                ServerUriFixedName,
                serverUri,
                ClientIdFixedName,
                clientId,
                throwableMessage
        );
        messageAdapter.showMessage(message);
    }
    private void showClientConnectionLost(String serverUri, String clientId) {
        String message = String.format("%s: Connection LOST (%s: %s, %s: %s).",
                new Date(),
                ServerUriFixedName,
                serverUri,
                ClientIdFixedName,
                clientId
        );
        messageAdapter.showMessage(message);
    }
    //-------------------------------------------------------------------------
    private void showSubscribeAdded(String topicName, int qos) {
        String message = String.format("%s: +SUBSCRIBE (%s: %s, %s: %s, %s: %s, QoS: %s).",
                new Date(),
                ServerUriFixedName,
                serverUri,
                ClientIdFixedName,
                clientId,
                KahaDestination.DestinationType.TOPIC,
                topicName,
                qosToString(qos)
        );
        messageAdapter.showMessage(message);
    }
    private void showSubscribeRemoved(String topicName) {
        String message = String.format("%s: -SUBSCRIBE (%s: %s, %s: %s, %s: %s).",
                new Date(),
                ServerUriFixedName,
                serverUri,
                ClientIdFixedName,
                clientId,
                KahaDestination.DestinationType.TOPIC,
                topicName
        );
        messageAdapter.showMessage(message);
    }
    //endregion
    public MqttConsumer(IMessageAdapter messageAdapter) {
        if(messageAdapter == null) {
            throw new NullPointerException("messageAdapter");
        }

        this.messageAdapter = messageAdapter;
    }

    //-------------------------------------------------------------------------
    public static final int DefaultQoS = 2;
    //-------------------------------------------------------------------------
    public boolean isConnected() {
        return clientId != null;
    }
    //-------------------------------------------------------------------------
    public void connect(String clientId) throws InvalidObjectException {
        String serverUri = null;

        try {
            InetAddress inet = InetAddress.getLocalHost();
            InetAddress[] ips = InetAddress.getAllByName(inet.getCanonicalHostName());
            if (ips != null && ips.length != 0) {
                serverUri = "tcp://" + ips[0].getHostAddress();
            }
            else {
                throw new InvalidDestinationException("Not network device.");
            }
        }
        catch (Throwable throwable) {
            showException(throwable);
        }

        if(serverUri != null) {
            connect(clientId, serverUri);
        }
    }
    public void connect(String clientId, String serverUri) throws InvalidObjectException {
        if(isNullOrEmpty(clientId)) {
            throw new NullPointerException("clientId");
        }
        if(isNullOrEmpty(serverUri)) {
            throw new NullPointerException("serverUri");
        }
        if(isConnected()) {
            throw new InvalidObjectException("Already connected.");
        }

        Throwable throwable = null;
        try {
            this.serverUri = serverUri;
            this.clientId = clientId;

            mqttClient = new MqttClient(serverUri, clientId);
            mqttClient.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable throwable) {
                    showClientConnectionLost(serverUri, clientId);
                }

                @Override
                public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
                    messageAdapter.showMessage(new MessageInfo(mqttMessage, s, serverUri, clientId));
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
            showClientConnected(serverUri, clientId, throwable);

            if(throwable != null) {
                disconnect();
            }
        }
    }

    public void disconnect() {
        if(isConnected()) {
            Throwable throwable = null;
            String tmpServerUri = serverUri;
            String tmpClientId = clientId;

            try {
                if(mqttClient != null) {
                    mqttClient.disconnect(1000);
                }
            }
            catch (Throwable e) {
                throwable = e;
            }
            finally {
                serverUri = null;
                clientId = null;
                showClientDisconnected(tmpServerUri, tmpClientId, throwable);
            }
        }
    }
    //-------------------------------------------------------------------------
    public void addTopicAfterConnect(String topicName) throws InvalidObjectException {
        addTopicAfterConnect(topicName, DefaultQoS);
    }
    public void addTopicAfterConnect(String topicName, int qos) throws InvalidObjectException {
        if(isNullOrEmpty(topicName)) {
            throw new NullPointerException("topicName");
        }
        if(!isConnected()) {
            throw new InvalidObjectException("Is not connected.");
        }

        try {
            mqttClient.subscribe(topicName, qos);
            showSubscribeAdded(topicName, qos);
        }
        catch (Throwable throwable) {
            showException(throwable);
        }
    }
    public void removeTopicAfterConnect(String topicName) throws InvalidObjectException {
        if(isNullOrEmpty(topicName)) {
            throw new NullPointerException("topicName");
        }
        if(!isConnected()) {
            throw new InvalidObjectException("Is not connected.");
        }

        try {
            mqttClient.unsubscribe(topicName);
            showSubscribeRemoved(topicName);
        }
        catch (Throwable throwable) {
            showException(throwable);
        }
    }
    //-------------------------------------------------------------------------
    public static String qosToString(int qos) {
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
    //-------------------------------------------------------------------------
    public final class MessageInfo {
        //region private
        private final String serverUri;
        private final String clientId;
        private final String message;
        private final int qos;
        private final KahaDestination.DestinationType destinationType;
        private final String topicName;
        //endregion
        MessageInfo(MqttMessage message, String topicName, String serverUri, String clientId) {
            this.message = new String(message.getPayload());
            this.qos = message.getQos();
            this.destinationType = KahaDestination.DestinationType.TOPIC;
            this.topicName = topicName;
            this.serverUri = serverUri;
            this.clientId = clientId;
        }

        //---------------------------------------------------------------------
        public String getMessage() {
            return message;
        }
        public int getQoS() {
            return qos;
        }
        public KahaDestination.DestinationType getDestinationType() {
            return destinationType;
        }
        public String getTopicName() {
            return topicName;
        }
        public String getServerUri() {
            return serverUri;
        }
        public String getClientId() {
            return clientId;
        }
        //---------------------------------------------------------------------
        public void show() {
            messageAdapter.showMessage(toString());
        }
        //---------------------------------------------------------------------
        @Override
        public String toString() {
            int messageLength = message.length();

            return String.format("%s: RECEIVED (%s: %s, %s: %s, %s: %s, QoS: %s, MsgLng: %s).",
                    new Date(),
                    ServerUriFixedName,
                    serverUri,
                    ClientIdFixedName,
                    clientId,
                    KahaDestination.DestinationType.TOPIC,
                    topicName,
                    qos,
                    messageLength
            );
        }
        //---------------------------------------------------------------------
    }
    //-------------------------------------------------------------------------
    public interface IMessageAdapter {
        void showMessage(String message);
        void showMessage(MessageInfo message);
    }
    //-------------------------------------------------------------------------
}
