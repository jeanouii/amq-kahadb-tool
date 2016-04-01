package activemq;


import org.apache.activemq.*;
import org.apache.activemq.Message;
import org.apache.activemq.command.*;

import javax.jms.*;
import java.io.InvalidObjectException;
import java.util.Date;
import java.util.HashMap;

public class Publisher {
    //region private
    private static final String ServerUri = ActiveMQConnection.DEFAULT_BROKER_URL;
    //-------------------------------------------------------------------------
    private static final String ClientIdName = "PublisherClientId";
    //-------------------------------------------------------------------------
    private final HashMap<String, Long> topicSendedMessageStatistic = new HashMap<>();
    //-------------------------------------------------------------------------
    private final String clientId;
    //-------------------------------------------------------------------------
    private ActiveMQConnection connection;
    private boolean connected;
    //-------------------------------------------------------------------------
    private void clientConnected(Throwable throwable) {
        if(throwable == null) {
            connected=  true;
            System.out.printf("%s: Connected (%s: %s).\r\n", new Date(), ClientIdName, clientId);
        }
        else {
            System.out.printf("%s: Connected FAILED (%s: %s).\r\nThrowable: %s.\r\n", new Date(), ClientIdName, clientId, throwable);
        }
    }
    private void clientDisconnected(Throwable throwable) {
        if(throwable == null) {
            connected = false;
            System.out.printf("%s: Disconnected (%s: %s).\r\n", new Date(), ClientIdName, clientId);
        }
        else {
            System.out.printf("%s: Disconnected FAILED (%s: %s).\r\nThrowable: %s.\r\n", new Date(), ClientIdName, clientId, throwable);
        }

    }
    //-------------------------------------------------------------------------
    private void sendMessageToTopicResult(String message, String topicName, Throwable throwable) {
        Long sendedMessageCount = topicSendedMessageStatistic.get(topicName);
        int messageSize = message.length();
        if(throwable == null) {
            System.out.printf("%s: SEND (%s: %s, Topic: %s (Sended: %s), MessageSize: %s).\r\n", new Date(), ClientIdName, clientId, topicName, sendedMessageCount, messageSize);
        }
        else {
            System.out.printf("%s: SEND FAILED (%s: %s, Topic: %s (Sended: %s), MessageSize: %s)", new Date(), ClientIdName, clientId, topicName, sendedMessageCount, messageSize);
        }
    }
    //-------------------------------------------------------------------------
    private void addTopicSendedMessageStatistic(String topicName) {
        Long sendedMessageCount = topicSendedMessageStatistic.get(topicName);
        sendedMessageCount = sendedMessageCount == null ? 0 : sendedMessageCount;
        ++sendedMessageCount;
        topicSendedMessageStatistic.put(topicName, sendedMessageCount);
    }
    //endregion
    public Publisher(String clientId) {
        if(clientId == null || clientId.length() == 0) {
            throw new NullPointerException("clientId");
        }

        this.clientId = clientId;
    }

    //-------------------------------------------------------------------------
    public boolean isConnected() {
        return connected;
    }
    //-------------------------------------------------------------------------
    public void connect() throws InvalidObjectException {
        if(connected) {
            throw new InvalidObjectException("Already connected");
        }

        try {
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ServerUri);
            connection = (ActiveMQConnection)connectionFactory.createConnection();
            connection.setClientID(clientId);

            clientConnected(null);
        }
        catch (JMSException exception) {
            clientConnected(exception);
        }
    }
    public void disconnect() {
        if(connected) {
            try {
                connection.close();
                clientConnected(null);
            }
            catch (JMSException exception) {
                clientDisconnected(exception);
            }
        }
    }
    //-------------------------------------------------------------------------
    public void sendMessageInTopic(String message, String topicName) throws InvalidObjectException {
        if(message == null || message.length() == 0) {
            throw new NullPointerException("message");
        }
        if(topicName == null || topicName.length() == 0) {
            throw new NullPointerException("topicName");
        }
        if(!connected) {
            throw new InvalidObjectException("You must call the function connect().");
        }

        Throwable throwable = null;
        try {
            ActiveMQSession session = (ActiveMQSession)connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ActiveMQTopic topic = (ActiveMQTopic)session.createTopic(topicName);
            ActiveMQMessageProducer messageProducer = (ActiveMQMessageProducer)session.createProducer(topic);

            TextMessage textMessage = session.createTextMessage(message);
            //messageProducer.send(textMessage, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, 5000);
            messageProducer.send(textMessage, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            addTopicSendedMessageStatistic(topicName);

            messageProducer.close();
            session.close();
        }
        catch (JMSException exception) {
            throwable = exception;
        }
        finally {
            sendMessageToTopicResult(message, topicName, throwable);
        }
    }
    //-------------------------------------------------------------------------
}
