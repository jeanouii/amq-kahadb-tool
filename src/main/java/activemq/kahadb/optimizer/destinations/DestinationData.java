package activemq.kahadb.optimizer.destinations;

import activemq.kahadb.optimizer.locations.MessageLocation;
import activemq.kahadb.optimizer.locations.SubscriptionLocation;
import org.apache.activemq.store.kahadb.disk.journal.Location;

import java.util.Iterator;
import java.util.LinkedList;

import static activemq.Utils.isNullOrEmpty;

public final class DestinationData {
    //region private
    private final String destinationId;
    private final LinkedList<SubscriptionLocation> subscriptionLocations = new LinkedList<>();
    private final LinkedList<MessageLocation> messageLocations = new LinkedList<>();
    //endregion
    public DestinationData(String destinationId) {
        if(isNullOrEmpty(destinationId)) {
            throw new NullPointerException("destinationId");
        }

        this.destinationId = destinationId;
    }

    //-------------------------------------------------------------------------
    String getDestinationId() {
        return destinationId;
    }
    //-------------------------------------------------------------------------
    boolean hasSubscriptions() {
        return subscriptionLocations.size() != 0;
    }
    boolean hasMessages() { return messageLocations.size() != 0; }
    //-------------------------------------------------------------------------
    SubscriptionLocation[] getSubscriptionLocations() {
        SubscriptionLocation[] arraySubscriptionLocations = new SubscriptionLocation[subscriptionLocations.size()];
        return subscriptionLocations.toArray(arraySubscriptionLocations);
    }
    MessageLocation[] getMessageLocations() {
        MessageLocation[] arrayMessageLocations = new MessageLocation[messageLocations.size()];
        return messageLocations.toArray(arrayMessageLocations);
    }
    //-------------------------------------------------------------------------
    public MessageLocation getMessageLocation(String messageId) {
        if(isNullOrEmpty(messageId)) {
            return null;
        }

        MessageLocation messageLocation = null;

        final Iterator<MessageLocation> iteratorMessageLocations = messageLocations.iterator();
        while (iteratorMessageLocations.hasNext()) {
            MessageLocation nextMessageLocation = iteratorMessageLocations.next();
            if (nextMessageLocation.getMessageId().equals(messageId)) {
                messageLocation = nextMessageLocation;
                break;
            }
        }

        return messageLocation;
    }
    //-------------------------------------------------------------------------
    boolean addSubscriptionLocation(String subscriptionKey, Location location, boolean retroactive) {
        return addSubscriptionLocation(new SubscriptionLocation(subscriptionKey, destinationId, location), retroactive);
    }
    boolean addSubscriptionLocation(SubscriptionLocation subscriptionLocation, boolean retroactive) {
        if(subscriptionLocation == null || !destinationId.equals(subscriptionLocation.getDestinationId())) {
            return false;
        }

        boolean removed = false;
        final Iterator<SubscriptionLocation> iteratorSubscriptionLocations = subscriptionLocations.iterator();
        while (iteratorSubscriptionLocations.hasNext()) {
            SubscriptionLocation nextSubscriptionLocation = iteratorSubscriptionLocations.next();
            if (nextSubscriptionLocation.getSubscriptionKey().equals(subscriptionLocation.getSubscriptionKey())) {
                if(nextSubscriptionLocation.getLocation().compareTo(subscriptionLocation.getLocation()) == 0) {
                    return false;
                }

                iteratorSubscriptionLocations.remove();
                removed = true;
                break;
            }
        }

        if(!removed) {
            if(messageLocations.size() != 0) {
                String subscriptionKey = subscriptionLocation.getSubscriptionKey();
                if (retroactive) {
                    for (MessageLocation messageLocation : messageLocations) {
                        messageLocation.addPendingSubscriptionKey(subscriptionKey);
                    }
                }
                else {
                    MessageLocation lastMessageLocation = messageLocations.getLast();
                    if (lastMessageLocation != null) {
                        lastMessageLocation.addPendingSubscriptionKey(subscriptionKey);
                    }
                }
            }
        }

        subscriptionLocations.add(subscriptionLocation);
        return true;
    }
    //-------------------------------------------------------------------------
    boolean addMessageLocation(String messageId, Location location) {
        MessageLocation messageLocation = hasSubscriptions()
                ? new MessageLocation(messageId, destinationId, location, getSubscriptionLocations())
                : new MessageLocation(messageId, destinationId, location);

        return addMessageLocation(messageLocation);
    }
    boolean addMessageLocation(MessageLocation messageLocation) {
        if(messageLocation == null || !destinationId.equals(messageLocation.getDestinationId())){
            return false;
        }

        final Iterator<MessageLocation> iteratorMessageLocations = messageLocations.iterator();
        while (iteratorMessageLocations.hasNext()) {

            if (iteratorMessageLocations.next().getMessageId().equals(messageLocation.getMessageId())) {
                return false;
            }
        }

        messageLocations.add(messageLocation);
        return true;
    }
    //-------------------------------------------------------------------------
    boolean removeSubscriptionLocation(String subscriptionKey) {
        if(isNullOrEmpty(subscriptionKey)) {
            return false;
        }

        SubscriptionLocation removedSubscriptionLocation = null;

        final Iterator<SubscriptionLocation> iteratorSubscriptionLocations = subscriptionLocations.iterator();
        while (iteratorSubscriptionLocations.hasNext()) {
            SubscriptionLocation nextSubscriptionLocation = iteratorSubscriptionLocations.next();
            if (nextSubscriptionLocation.getSubscriptionKey().equals(subscriptionKey)) {
                removedSubscriptionLocation = nextSubscriptionLocation;
                iteratorSubscriptionLocations.remove();
                break;
            }
        }

        if(removedSubscriptionLocation != null) {
            final Iterator<MessageLocation> iteratorMessageLocations = messageLocations.iterator();
            while (iteratorMessageLocations.hasNext()) {
                MessageLocation nextMessageLocation = iteratorMessageLocations.next();
                nextMessageLocation.removePendingSubscriptionKey(subscriptionKey);
                if(nextMessageLocation.hasAllAcks()) {
                    iteratorMessageLocations.remove();
                }
            }
        }

        return removedSubscriptionLocation != null;
    }

    boolean removeMessageLocation(String messageId) {
        if(isNullOrEmpty(messageId)) {
            return false;
        }

        boolean removed = false;

        final Iterator<MessageLocation> iteratorMessageLocations = messageLocations.iterator();
        while (iteratorMessageLocations.hasNext()) {
            if (iteratorMessageLocations.next().getMessageId().equals(messageId)) {
                iteratorMessageLocations.remove();
                removed = true;
                break;
            }
        }

        return removed;
    }
    boolean removeMessageLocation(String messageId, String subscriptionKey, Location ackLocation) {
        if(isNullOrEmpty(messageId)) {
            return false;
        }

        boolean removed = false;

        final Iterator<MessageLocation> iteratorMessageLocations = messageLocations.iterator();
        while (iteratorMessageLocations.hasNext()) {
            MessageLocation nextMessageLocation = iteratorMessageLocations.next();
            if (nextMessageLocation.getMessageId().equals(messageId)) {
                nextMessageLocation.addAckLocation(subscriptionKey, ackLocation);
                if (nextMessageLocation.hasAllAcks()) {
                    iteratorMessageLocations.remove();
                    removed = true;
                }
                break;
            }
        }

        return removed;
    }
    //-------------------------------------------------------------------------
}
