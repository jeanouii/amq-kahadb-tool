package org.hill30.activemq.clients;

import org.apache.activemq.*;
import org.apache.activemq.store.kahadb.data.KahaDestination;

import javax.jms.*;
import javax.jms.Message;
import javax.management.OperationsException;
import java.io.InvalidObjectException;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;

import static org.hill30.activemq.Utils.*;

public class Consumer {
    //region private
    private static final String ServerUriFixedName = "SrvUri";
    public static final String ClientIdFixedName = "ClntId";
    //-------------------------------------------------------------------------
    private final LinkedList<InternalDestination> destinations = new LinkedList<>();
    private final IMessageAdapter messageAdapter;
    //-------------------------------------------------------------------------
    private ActiveMQConnection connection;
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
    //-------------------------------------------------------------------------
    private final class InternalDestination {
        //region private
        private final KahaDestination.DestinationType destinationType;
        private final String destinationName;
        //---------------------------------------------------------------------
        private InternalSubscription subscription;
        private boolean useTransaction;
        private ActiveMQSession session;
        private ActiveMQMessageConsumer consumer;
        //---------------------------------------------------------------------
        private void showSubscribeAdded() throws JMSException {
            String subscriptionInfo = ", SubName: " + subscription.getSubscriptionName();
            if(subscription.getMessageSelector() != null) {
                subscriptionInfo += ", MessageSelector: " + subscription.getMessageSelector();
            }
            if(subscription.getNoLocal() != DefaultSubscriptionNoLocal) {
                subscriptionInfo += ", NoLocal: " + subscription.getNoLocal();
            }

            String message = String.format("%s: +SUBSCRIBE (%s: %s, %s: %s, %s: %s%s).",
                    new Date(),
                    ServerUriFixedName,
                    serverUri,
                    ClientIdFixedName,
                    clientId,
                    destinationType,
                    destinationName,
                    subscriptionInfo
            );
            messageAdapter.showMessage(message);
        }
        private void showSubscribeRemoved() {
            String subscriptionInfo = ", SubName: " + subscription.getSubscriptionName();
            String message = String.format("%s: -SUBSCRIBE (%s: %s, %s: %s, %s: %s%s).\r\n",
                    new Date(),
                    ServerUriFixedName,
                    serverUri,
                    ClientIdFixedName,
                    clientId,
                    destinationType,
                    destinationName,
                    subscriptionInfo
            );
            messageAdapter.showMessage(message);
        }
        //endregion
        InternalDestination(KahaDestination.DestinationType destinationType, String destinationName) {
            if(isNullOrEmpty(destinationName)) {
                throw new NullPointerException("destinationName");
            }

            this.destinationType = destinationType;
            this.destinationName = destinationName;
        }

        //---------------------------------------------------------------------
        boolean hasSubscription() {
            return subscription != null;
        }
        boolean isSetSubscriptionAvailable() {
            return destinationType == KahaDestination.DestinationType.TOPIC;
        }
        //---------------------------------------------------------------------
        KahaDestination.DestinationType getDestinationType() {
            return destinationType;
        }
        String getDestinationName() {
            return destinationName;
        }
        boolean getUseTransaction() {
            return useTransaction;
        }
        InternalSubscription getSubscription() {
            return  subscription;
        }
        ActiveMQMessageConsumer getConsumer() {
            return consumer;
        }
        //---------------------------------------------------------------------
        void setUseTransaction(boolean useTransaction) {
            this.useTransaction = useTransaction;
        }
        void setSubscription(InternalSubscription subscription) throws InvalidObjectException {
            if(!isSetSubscriptionAvailable()) {
                throw new InvalidObjectException("subscription");
            }
            if(isConnected()) {
                throw new InvalidObjectException("Is connected.");
            }

            this.subscription = subscription;
        }
        //---------------------------------------------------------------------
        boolean removeSubsription() throws JMSException {
            if(!hasSubscription()) {
                return false;
            }

            if(isConnected()) {
                try {
                    consumer.close();
                    session.unsubscribe(subscription.getSubscriptionName());
                    showSubscribeRemoved();
                }
                catch (JMSException e) {
                    e.printStackTrace();
                    return false;
                }
            }

            subscription = null;
            return true;
        }
        //---------------------------------------------------------------------
        void setConnection(ActiveMQConnection connection) throws OperationsException, JMSException {
            this.session = (ActiveMQSession)connection.createSession(getUseTransaction(), Session.AUTO_ACKNOWLEDGE);

            if(destinationType == KahaDestination.DestinationType.TOPIC) {
                Topic topic = session.createTopic(java.lang.String.valueOf(destinationName));

                if(hasSubscription()) {
                    consumer = (ActiveMQMessageConsumer)session.createDurableSubscriber(topic, subscription.getSubscriptionName(), subscription.getMessageSelector(), subscription.getNoLocal());
                    showSubscribeAdded();
                }
                else {
                    consumer = (ActiveMQMessageConsumer)session.createConsumer(topic);
                }
            }
            else {
                Queue queue = session.createQueue(destinationName);
                consumer = (ActiveMQMessageConsumer)session.createConsumer(queue);
            }
        }
        //---------------------------------------------------------------------
    }
    private final class InternalSubscription {
        //region private
        private final String subscriptionName;
        private final String messageSelector;
        private final boolean noLocal;
        //endregion
        InternalSubscription(String subscriptionName, String messageSelector, boolean noLocal) {
            this.subscriptionName = subscriptionName;
            this.messageSelector = messageSelector;
            this.noLocal = noLocal;
        }
        //---------------------------------------------------------------------
        String getSubscriptionName() {
            return subscriptionName;
        }
        String getMessageSelector() {
            return messageSelector;
        }
        boolean getNoLocal() {
            return noLocal;
        }
        //---------------------------------------------------------------------
    }
    //-------------------------------------------------------------------------
    private void addDestination(KahaDestination.DestinationType destinationType, String destinationName, boolean useTransaction, InternalSubscription subscription) throws InvalidObjectException {
        InternalDestination destination = getDestination(destinationType, destinationName);

        if(destination == null) {
            destination = new InternalDestination(destinationType, destinationName);
            destinations.add(destination);
        }

        destination.setUseTransaction(useTransaction);

        if(destination.isSetSubscriptionAvailable()) {
            destination.setSubscription(subscription);
        }
    }
    private boolean removeDestination(KahaDestination.DestinationType destinationType, String destinationName) {
        final Iterator<InternalDestination> iteratorDestinations = destinations.iterator();
        while (iteratorDestinations.hasNext()) {
            InternalDestination nextDestination = iteratorDestinations.next();
            if(nextDestination.getDestinationType().equals(destinationType) && nextDestination.getDestinationName().equals(destinationName)) {
                if(isConnected()) {
                    try {
                        nextDestination.getConsumer().close();
                        nextDestination.removeSubsription();
                    } catch (JMSException e) {
                        e.printStackTrace();
                        return false;
                    }
                }

                iteratorDestinations.remove();
                return true;
            }
        }
        return false;
    }
    private InternalDestination getDestination(KahaDestination.DestinationType destinationType, String destinationName) {
        for(InternalDestination destination : destinations) {
            if(destination.getDestinationType().equals(destinationType) && destination.getDestinationName().equals(destinationName)) {
                return destination;
            }
        }
        return null;
    }
    //-------------------------------------------------------------------------
    private MessageInfo intGetMessage(KahaDestination.DestinationType destinationType, String destinationName, int timeout) {
        try {
            for (InternalDestination destination : destinations) {
                if(destination.getDestinationType().equals(destinationType)) {
                    if(isNullOrEmpty(destinationName) || destination.getDestinationName().equals(destinationName)) {
                        Message message = destination.getConsumer().receive(timeout);
                        if (message != null) {
                            return new MessageInfo((TextMessage) message, destination, serverUri, clientId);
                        }
                        break;
                    }
                }
            }

        } catch (JMSException e) {
            e.printStackTrace();
        }

        return null;
    }
    private MessageInfo intGetMessage(int timeout) {
        try {
            for (InternalDestination destination : destinations) {
                Message message = destination.getConsumer().receive(timeout);
                if(message != null) {
                    return new MessageInfo((TextMessage)message, destination, serverUri, clientId);
                }
            }

        } catch (JMSException e) {
            e.printStackTrace();
        }

        return null;
    }
    //endregion
    public Consumer(IMessageAdapter messageAdapter) {
        if(messageAdapter == null) {
            throw new NullPointerException("messageAdapter");
        }

        this.messageAdapter = messageAdapter;
    }

    //-------------------------------------------------------------------------
    public static final boolean DefaultUseTransaction = false;
    public static final String DefaultSubscriptionMessageSelector = null;
    public static final boolean DefaultSubscriptionNoLocal = false;
    public static final int DefaultTimeout = 100;
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

        try {
            this.serverUri = serverUri;
            this.clientId = clientId;

            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(serverUri);
            connection = (ActiveMQConnection)connectionFactory.createConnection();
            connection.setClientID(clientId);

            showClientConnected(serverUri, clientId, null);

            for (InternalDestination destination : destinations) {
               destination.setConnection(connection);
            }

            connection.start();
        }
        catch (Throwable throwable) {
            showClientConnected(serverUri, clientId, throwable);
            disconnect();
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
    public void addTopicBeforeConnect(String topicName) throws InvalidObjectException {
        addTopicBeforeConnect(topicName, DefaultUseTransaction, "", DefaultSubscriptionMessageSelector, DefaultSubscriptionNoLocal);
    }
    public void addTopicBeforeConnect(String topicName, String subscriptionName) throws InvalidObjectException {
        addTopicBeforeConnect(topicName, DefaultUseTransaction, subscriptionName, DefaultSubscriptionMessageSelector, DefaultSubscriptionNoLocal);
    }
    public void addTopicBeforeConnect(String topicName, boolean useTransaction) throws InvalidObjectException {
        addTopicBeforeConnect(topicName, useTransaction, "", DefaultSubscriptionMessageSelector, DefaultSubscriptionNoLocal);
    }
    public void addTopicBeforeConnect(String topicName, boolean useTransaction, String subscriptionName) throws InvalidObjectException {
        addTopicBeforeConnect(topicName, useTransaction, subscriptionName, DefaultSubscriptionMessageSelector, DefaultSubscriptionNoLocal);
    }
    public void addTopicBeforeConnect(String topicName, boolean useTransaction, String subscriptionName, String subscriptionMessageSelector, boolean subscriptionNoLocal) throws InvalidObjectException {
        if(isNullOrEmpty(topicName)) {
            throw new NullPointerException("topicName");
        }
        if(isConnected()) {
            throw new InvalidObjectException("Is connected.");
        }

        InternalSubscription subscription = null;
        if(!isNullOrEmpty(subscriptionName)) {
            for (InternalDestination destination : destinations) {
                if (destination.hasSubscription()) {
                    if (destination.getSubscription().getSubscriptionName().equals(subscriptionName)) {
                        throw new InvalidObjectException("subscriptionName");
                    }
                }
            }
            subscription = new InternalSubscription(subscriptionName, subscriptionMessageSelector, subscriptionNoLocal);
        }

        addDestination(KahaDestination.DestinationType.TOPIC,  topicName, useTransaction, subscription);
    }
    //-------------------------------------------------------------------------
    public void addQueueBeforeConnect(String queueName) throws InvalidObjectException {
        addQueueBeforeConnect(queueName, DefaultUseTransaction);
    }
    public void addQueueBeforeConnect(String queueName, boolean useTransaction) throws InvalidObjectException {
        if(isNullOrEmpty(queueName)) {
            throw new NullPointerException("topicName");
        }
        if(isConnected()) {
            throw new InvalidObjectException("Is connected.");
        }

        addDestination(KahaDestination.DestinationType.QUEUE, queueName, useTransaction, null);
    }
    //-------------------------------------------------------------------------
    public boolean removeTopic(String topicName) {
        if(isNullOrEmpty(topicName)) {
            return false;
        }

        return removeDestination(KahaDestination.DestinationType.TOPIC, topicName);
    }
    public boolean removeQueue(String queue) {
        if(isNullOrEmpty(queue)) {
            return false;
        }

        return removeDestination(KahaDestination.DestinationType.QUEUE, queue);
    }
    //-------------------------------------------------------------------------
    public MessageInfo getTopicsMessage() throws InvalidObjectException {
        return getTopicsMessage(DefaultTimeout);
    }
    public MessageInfo getTopicsMessage(int timeout) throws InvalidObjectException {
        if(timeout < 0) {
            throw new ArrayIndexOutOfBoundsException("timeout");
        }
        if(!isConnected()) {
            throw new InvalidObjectException("Is not connected.");
        }

        return intGetMessage(KahaDestination.DestinationType.TOPIC, null, timeout);
    }
    public MessageInfo getTopicMessage(String topicName) throws InvalidObjectException {
        return getTopicMessage(topicName, DefaultTimeout);
    }
    public MessageInfo getTopicMessage(String topicName, int timeout) throws InvalidObjectException {
        if(isNullOrEmpty(topicName)) {
            throw new NullPointerException("topicName");
        }
        if(timeout < 0) {
            throw new ArrayIndexOutOfBoundsException("timeout");
        }
        if(!isConnected()) {
            throw new InvalidObjectException("Is not connected.");
        }

        return intGetMessage(KahaDestination.DestinationType.TOPIC, topicName, timeout);
    }
    //-------------------------------------------------------------------------
    public MessageInfo getQueuesMessage() throws InvalidObjectException {
        return getQueuesMessage(DefaultTimeout);
    }
    public MessageInfo getQueuesMessage(int timeout) throws InvalidObjectException {
        if(timeout < 0) {
            throw new ArrayIndexOutOfBoundsException("timeout");
        }
        if(!isConnected()) {
            throw new InvalidObjectException("Is not connected.");
        }

        return intGetMessage(KahaDestination.DestinationType.QUEUE, null, timeout);
    }
    public MessageInfo getQueueMessage(String queueName) throws InvalidObjectException {
        return getQueueMessage(queueName, DefaultTimeout);
    }
    public MessageInfo getQueueMessage(String queueName, int timeout) throws InvalidObjectException {
        if(isNullOrEmpty(queueName)) {
            throw new NullPointerException("queueName");
        }
        if(timeout < 0) {
            throw new ArrayIndexOutOfBoundsException("timeout");
        }
        if(!isConnected()) {
            throw new InvalidObjectException("Is not connected.");
        }

        return intGetMessage(KahaDestination.DestinationType.QUEUE, queueName, timeout);
    }
    //-------------------------------------------------------------------------
    public MessageInfo getMessage() throws InvalidObjectException {
        return getMessage(DefaultTimeout);
    }
    public MessageInfo getMessage(int timeout) throws InvalidObjectException {
        if(timeout < 0) {
            throw new ArrayIndexOutOfBoundsException("timeout");
        }
        if(!isConnected()) {
            throw new InvalidObjectException("Is not connected.");
        }

        return intGetMessage(timeout);
    }
    //-------------------------------------------------------------------------
    public final class MessageInfo {
        //region private
        private static final int DefaultPriority = org.apache.activemq.Message.DEFAULT_PRIORITY;
        //---------------------------------------------------------------------
        private final String serverUri;
        private final String clientId;
        private final String message;
        private final int priority;
        private final KahaDestination.DestinationType destinationType;
        private final String destinationName;
        private final boolean useTransaction;
        private final String subscriptionName;
        //endregion
        MessageInfo(TextMessage message, InternalDestination destination, String serverUri, String clientId) throws JMSException {
            this.message = message.getText();
            this.priority = message.getJMSPriority();
            this.destinationType = destination.getDestinationType();
            this.destinationName = destination.getDestinationName();
            this.useTransaction = destination.getUseTransaction();
            this.subscriptionName = destination.hasSubscription() ? destination.getSubscription().getSubscriptionName() : null;
            this.serverUri = serverUri;
            this.clientId = clientId;
        }

        //---------------------------------------------------------------------
        public boolean hasSubscription() {
            return subscriptionName != null;
        }
        //---------------------------------------------------------------------
        public String getMessage() {
            return message;
        }
        public int getPriority() {
            return priority;
        }
        public KahaDestination.DestinationType getDestinationType() {
            return destinationType;
        }
        public String getDestinationName() {
            return destinationName;
        }
        public String getSubscriptionName() {
            return subscriptionName;
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
            String priorityMessage = priority == DefaultPriority ? "" : ", Priority: " + priority;
            String useTransactionMessage = useTransaction == DefaultUseTransaction ? "" : ", Trans: " + useTransaction;
            String subscriptionNameMessage = hasSubscription() ? ", SubName: " + subscriptionName : "";

            return String.format("%s: RECEIVED (%s: %s, %s: %s, %s: %s%s%s%s, MsgLn: %s).",
                    new Date(),
                    ServerUriFixedName,
                    serverUri,
                    ClientIdFixedName,
                    clientId,
                    destinationType,
                    destinationName,
                    priorityMessage,
                    useTransactionMessage,
                    subscriptionNameMessage,
                    messageLength
            );
        }
        //---------------------------------------------------------------------
    }
    //-------------------------------------------------------------------------
    public interface IMessageAdapter {
        void showMessage(String message);
    }
    //-------------------------------------------------------------------------
}

