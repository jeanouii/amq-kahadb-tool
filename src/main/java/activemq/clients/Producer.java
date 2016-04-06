package activemq.clients;

import org.apache.activemq.*;
import org.apache.activemq.Message;
import org.apache.activemq.store.kahadb.data.KahaDestination;

import javax.jms.*;
import java.io.InvalidObjectException;
import java.util.Date;

import static activemq.Utils.*;

public class Producer {
    //region private
    private static final String ClientIdName = "ProClientId";
    //-------------------------------------------------------------------------
    private ActiveMQConnection connection;
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
    //-------------------------------------------------------------------------
    private void sendMessageInDestination(String message, KahaDestination.DestinationType destinationType, String destinationName, boolean useTransaction) throws InvalidObjectException {
        Throwable throwable = null;
        ActiveMQSession session = null;
        ActiveMQMessageProducer producer = null;

        try {
            session = (ActiveMQSession)connection.createSession(useTransaction, Session.AUTO_ACKNOWLEDGE);
            Destination destination = destinationType == KahaDestination.DestinationType.TOPIC
                    ? session.createTopic(destinationName)
                    : session.createQueue(destinationName);
            producer = (ActiveMQMessageProducer)session.createProducer(destination);

            TextMessage textMessage = session.createTextMessage(message);
            for(int i = 0; i < 5; ++i) {
                producer.send(textMessage, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                sendMessageToDestinationResult(message, destinationType, destinationName);
            }
            if(useTransaction) {
                session.commit();
            }
        }
        catch (JMSException exception) {
            throwable = exception;
        }
        finally {
            if(producer != null) {
                try {
                    producer.close();
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }

            if(session != null) {
                try {
                    session.close();
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }

            if(throwable != null) {
                showException(throwable);
            }
        }
    }
    private void sendMessageToDestinationResult(String message, KahaDestination.DestinationType destinationType, String destinationName) {
        int messageSize = message.length();
        System.out.printf("%s: SEND (%s: %s, %s: %s, MessageSize: %s).\r\n", new Date(), ClientIdName, clientId, destinationType, destinationName, messageSize);
    }
    //endregion
    //-------------------------------------------------------------------------
    public boolean isConnected() {
        return clientId != null;
    }
    //-------------------------------------------------------------------------
    public void connect(String clientId) throws InvalidObjectException {
        connect(clientId, ActiveMQConnection.DEFAULT_BROKER_URL);
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
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(serverUri);
            connection = (ActiveMQConnection)connectionFactory.createConnection();
            connection.setClientID(clientId);
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
                connection.close();
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
    public void sendMessageInTopic(String message, String topicName, boolean useTransaction) throws InvalidObjectException {
        if(isNullOrEmpty(message)) {
            throw new NullPointerException("message");
        }
        if(isNullOrEmpty(topicName)) {
            throw new NullPointerException("topicName");
        }
        if(!isConnected()) {
            throw new InvalidObjectException("You must call the function connect().");
        }

        sendMessageInDestination(message, KahaDestination.DestinationType.TOPIC, topicName, useTransaction);
    }
    public void sendMessageInQueue(String message, String queueName, boolean useTransaction) throws InvalidObjectException {
        if(isNullOrEmpty(message)) {
            throw new NullPointerException("message");
        }
        if(isNullOrEmpty(queueName)) {
            throw new NullPointerException("queueName");
        }
        if(!isConnected()) {
            throw new InvalidObjectException("You must call the function connect().");
        }

        sendMessageInDestination(message, KahaDestination.DestinationType.QUEUE, queueName, useTransaction);
    }
    //-------------------------------------------------------------------------
}
