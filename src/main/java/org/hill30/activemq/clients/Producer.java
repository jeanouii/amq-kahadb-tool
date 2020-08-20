package org.hill30.activemq.clients;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageProducer;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.Message;
import org.apache.activemq.store.kahadb.data.KahaDestination;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.InvalidObjectException;
import java.util.Date;

import static org.hill30.activemq.Utils.isNullOrEmpty;
import static org.hill30.activemq.Utils.showException;

public class Producer {
    //region private
    private static final String ServerUriFixedName = "SrvUri";
    private static final String ClientIdFixedName =  "ProClntId";
    //-------------------------------------------------------------------------
    private final IMessageAdapter messageAdapter;
    //-------------------------------------------------------------------------
    private ActiveMQConnection connection;
    private String serverUri;
    private String clientId;
    //-------------------------------------------------------------------------
    private void sendMessage(
            String message,
            KahaDestination.DestinationType destinationType,
            String destinationName,
            boolean useTransaction,
            int priority,
            long timeToLive
    ) {
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
            producer.send(textMessage, DeliveryMode.PERSISTENT, priority, timeToLive);
            showMessageSended(textMessage, destinationType, destinationName, useTransaction);

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
    //-------------------------------------------------------------------------
    private void showMessageSended(TextMessage textMessage, KahaDestination.DestinationType destinationType, String destinationName, boolean useTransaction) throws JMSException {
        int messageLength = textMessage.getText().length();
        int priority = textMessage.getJMSPriority();
        String priorityMessage = priority == DefaultPriority ? "" : ", Priority: " + priority;
        String useTransactionMessage = useTransaction == DefaultUseTransaction ? "" : ", Trans: " + useTransaction;

        String message = String.format("%s: SENDED (%s: %s, %s: %s, %s: %s%s%s, MsgLn: %s).",
                new Date(),
                ServerUriFixedName,
                serverUri,
                ClientIdFixedName,
                clientId,
                destinationType,
                destinationName,
                priorityMessage,
                useTransactionMessage,
                messageLength
        );
        messageAdapter.showMessage(message);
    }
    //endregion
    public Producer(IMessageAdapter messageAdapter) {
        if(messageAdapter == null) {
            throw new NullPointerException("messageAdapter");
        }

        this.messageAdapter = messageAdapter;
    }

    //-------------------------------------------------------------------------
    public static final boolean DefaultUseTransaction = false;
    public static final int DefaultPriority = Message.DEFAULT_PRIORITY;
    public static final long DefaultTimeToLive = Message.DEFAULT_TIME_TO_LIVE;
    //-------------------------------------------------------------------------
    public String getClientId() {
        return clientId;
    }
    public boolean isConnected() {
        return clientId != null;
    }
    //-------------------------------------------------------------------------
    public void connect(String clientId) throws InvalidObjectException {
        connect(clientId, ActiveMQConnectionFactory.DEFAULT_BROKER_BIND_URL);
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

            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(serverUri);
            connection = (ActiveMQConnection)connectionFactory.createConnection();
            connection.setClientID(clientId);
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
                if(connection != null) {
                    connection.close();
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
    public void sendMessageInTopic(String message, String topicName) throws InvalidObjectException {
        sendMessageInTopic(message, topicName, DefaultUseTransaction, DefaultPriority, DefaultTimeToLive);
    }
    public void sendMessageInTopic(String message, String topicName, boolean useTransaction) throws InvalidObjectException {
        sendMessageInTopic(message, topicName, useTransaction, DefaultPriority, DefaultTimeToLive);
    }
    public void sendMessageInTopic(String message, String topicName, boolean useTransaction, int priority) throws InvalidObjectException {
        sendMessageInTopic(message, topicName, useTransaction, priority, DefaultTimeToLive);
    }
    public void sendMessageInTopic(String message, String topicName, boolean useTransaction, int priority, long timeToLive) throws InvalidObjectException {
        if(isNullOrEmpty(message)) {
            throw new NullPointerException("message");
        }
        if(isNullOrEmpty(topicName)) {
            throw new NullPointerException("topicName");
        }
        if(timeToLive < 0L) {
            throw new ArrayIndexOutOfBoundsException("timeToLive");
        }
        if(priority < 0) {
            throw new ArrayIndexOutOfBoundsException("priority");
        }
        if(!isConnected()) {
            throw new InvalidObjectException("Is not connected.");
        }

        sendMessage(message, KahaDestination.DestinationType.TOPIC, topicName, useTransaction, priority, timeToLive);
    }
    //-------------------------------------------------------------------------
    public void sendMessageInQueue(String message, String queueName) throws InvalidObjectException {
        sendMessageInQueue(message, queueName, DefaultUseTransaction, DefaultPriority, DefaultTimeToLive);
    }
    public void sendMessageInQueue(String message, String queueName, boolean useTransaction) throws InvalidObjectException {
        sendMessageInQueue(message, queueName, useTransaction, DefaultPriority, DefaultTimeToLive);
    }
    public void sendMessageInQueue(String message, String queueName, boolean useTransaction, int priority) throws InvalidObjectException {
        sendMessageInQueue(message, queueName, useTransaction, priority, DefaultTimeToLive);
    }
    public void sendMessageInQueue(String message, String queueName, boolean useTransaction, int priority, long timeToLive) throws InvalidObjectException {
        if(isNullOrEmpty(message)) {
            throw new NullPointerException("message");
        }
        if(isNullOrEmpty(queueName)) {
            throw new NullPointerException("queueName");
        }
        if(timeToLive < 0L) {
            throw new ArrayIndexOutOfBoundsException("timeToLive");
        }
        if(priority < 0) {
            throw new ArrayIndexOutOfBoundsException("priority");
        }
        if(!isConnected()) {
            throw new InvalidObjectException("Is not connected.");
        }

        sendMessage(message, KahaDestination.DestinationType.QUEUE, queueName, useTransaction, priority, timeToLive);
    }
    //-------------------------------------------------------------------------
    public interface IMessageAdapter {
        void showMessage(String message);
    }
    //-------------------------------------------------------------------------
}
