package activemq.clients;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.store.kahadb.data.KahaDestination;

import javax.jms.*;
import java.io.InvalidObjectException;
import java.util.Date;

import static activemq.Utils.isNullOrEmpty;
import static activemq.Utils.showException;

public class Consumer {
    //region private
    private static final String ClientIdName = "ClientId";
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
    private void receiveMessageOfDestination(KahaDestination.DestinationType destinationType, String destinationName, String subscribeName, boolean useTransaction) throws InvalidObjectException {
        Throwable throwable = null;
        ActiveMQSession session = null;
        ActiveMQMessageConsumer consumer = null;

        try {
            session = (ActiveMQSession)connection.createSession(useTransaction, Session.AUTO_ACKNOWLEDGE);
            Destination destination;
            if(destinationType == KahaDestination.DestinationType.TOPIC) {
                destination = session.createTopic(destinationName);
                TopicSubscriber topicSubscriber = session.createDurableSubscriber((Topic)destination, subscribeName);
                showSubscribeDestination(destinationType, destinationName, topicSubscriber);
            }
            else {
                destination = session.createQueue(destinationName);
            }

            consumer = (ActiveMQMessageConsumer)session.createConsumer(destination);

            javax.jms.Message message;
            while ((message = consumer.receive(1000)) != null) {
                showReceivedMessage(destinationType, destinationName, message);
            }

            if(useTransaction) {
                session.commit();
            }
        }
        catch (Throwable e) {
            throwable = e;
        }
        finally {
            if(consumer != null) {
                try {
                    consumer.close();
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
    private void unsubscribeFromDestination(String subscribeName, boolean useTransaction) throws InvalidObjectException {
        Throwable throwable = null;
        ActiveMQSession session = null;

        try {
            session = (ActiveMQSession)connection.createSession(useTransaction, Session.AUTO_ACKNOWLEDGE);
            session.unsubscribe(subscribeName);
            showUnsubscribeDestination(subscribeName);

            if(useTransaction) {
                session.commit();
            }
        }
        catch (Throwable e) {
            throwable = e;
        }
        finally {
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

    private void showSubscribeDestination(KahaDestination.DestinationType destinationType, String destinationName, TopicSubscriber topicSubscriber) throws JMSException {
        boolean isLocal = topicSubscriber != null && !topicSubscriber.getNoLocal();
        System.out.printf("%s: +SUBSCRIBE (%s: %s, %s: %s, isLocal: %s).\r\n", new Date(), ClientIdName, clientId, destinationType, destinationName, isLocal);
    }
    private void showUnsubscribeDestination(String destinationName) {
        String destinationType = KahaDestination.DestinationType.TOPIC.toString();
        System.out.printf("%s: -SUBSCRIBE (%s: %s, %s: %s).\r\n", new Date(), ClientIdName, clientId, destinationType, destinationName);
    }
    private void showReceivedMessage(KahaDestination.DestinationType destinationType, String destinationName, javax.jms.Message message) {
        int messageSize = 0;
        if(message instanceof org.apache.activemq.command.Message) {
            messageSize = ((org.apache.activemq.command.Message)message).getSize();
        }
        System.out.printf("%s: RECEIVED (%s: %s, %s: %s, MessageSize: %s).\r\n", new Date(), ClientIdName, clientId, destinationType, destinationName, messageSize);
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
    public void receiveMessagesOfQueue(String queueName, boolean useTransaction) throws InvalidObjectException {
        if(isNullOrEmpty(queueName)) {
            throw new NullPointerException("queueName");
        }
        if(!isConnected()) {
            throw new InvalidObjectException("You must call the function connect().");
        }

        receiveMessageOfDestination(KahaDestination.DestinationType.QUEUE, queueName, null, useTransaction);
    }
    //-------------------------------------------------------------------------
    public void subscribe(String topicName, String subscribeName, boolean useTransaction) throws InvalidObjectException {
        if(isNullOrEmpty(topicName)) {
            throw new NullPointerException("topicName");
        }
        if(isNullOrEmpty(subscribeName)) {
            throw new NullPointerException("subscribeName");
        }
        if(!isConnected()) {
            throw new InvalidObjectException("You must call the function connect().");
        }

        receiveMessageOfDestination(KahaDestination.DestinationType.TOPIC, topicName, subscribeName, useTransaction);
    }
    public void unsubscribe(String subscribeName, boolean useTransaction) throws InvalidObjectException {
        if(isNullOrEmpty(subscribeName)) {
            throw new NullPointerException("subscribeName");
        }
        if(!isConnected()) {
            throw new InvalidObjectException("You must call the function connect().");
        }

        unsubscribeFromDestination(subscribeName, useTransaction);
    }
    //-------------------------------------------------------------------------
}
