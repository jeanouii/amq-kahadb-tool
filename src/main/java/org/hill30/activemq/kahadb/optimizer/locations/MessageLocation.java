package org.hill30.activemq.kahadb.optimizer.locations;

import org.apache.activemq.store.kahadb.disk.journal.Location;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import static org.hill30.activemq.Utils.isNullOrEmpty;

public final class MessageLocation {
    //region private
    private final String messageId;
    private final String destinationId;
    //-------------------------------------------------------------------------
    private final Set<String> pendingAckSubscriptionKeys = new HashSet<>();
    private final LinkedList<AckMessageLocation> ackMessageLocations = new LinkedList<>();
    //-------------------------------------------------------------------------
    private Location location;
    //endregion
    public MessageLocation(String messageId, String destinationId, Location location) {
        this(messageId, destinationId, location, null);
    }
    public MessageLocation(String messageId, String destinationId, Location location, SubscriptionLocation[] subscriptionLocations) {
        if(isNullOrEmpty(messageId)) {
            throw new NullPointerException("messageId");
        }
        if(isNullOrEmpty(destinationId)) {
            throw new NullPointerException("destinationId");
        }
        if(location == null) {
            throw new NullPointerException("location");
        }

        this.messageId = messageId;
        this.destinationId = destinationId;
        this.location = location;

        if(subscriptionLocations != null) {
            for (SubscriptionLocation subscriptionLocation : subscriptionLocations) {
                pendingAckSubscriptionKeys.add(subscriptionLocation.getSubscriptionKey());
            }
        }
    }

    //-------------------------------------------------------------------------
    public String getMessageId() {
        return messageId;
    }
    public String getDestinationId() {
        return destinationId;
    }
    public Location getLocation() {
        return location;
    }
    //-------------------------------------------------------------------------
    public boolean hasAllAcks() {
        return pendingAckSubscriptionKeys.size() == 0 && ackMessageLocations.size() != 0;
    }
    //-------------------------------------------------------------------------
    public AckMessageLocation[] getAckMessageLocations() {
        AckMessageLocation[] result = new AckMessageLocation[ackMessageLocations.size()];
        return ackMessageLocations.toArray(result);
    }
    public void addAckLocation(String subscriptionKey, Location location) {
        if(isNullOrEmpty(subscriptionKey)) {
            throw new NullPointerException("subscriptionKey");
        }
        if(location == null) {
            throw new NullPointerException("location");
        }

        if(pendingAckSubscriptionKeys.remove(subscriptionKey)) {
            ackMessageLocations.add(new AckMessageLocation(subscriptionKey, location));
        }
    }
    //-------------------------------------------------------------------------
    public void addPendingSubscriptionKey(String subscriptionKey) {
        if(isNullOrEmpty(subscriptionKey)) {
            throw new NullPointerException("subscriptionKey");
        }

        pendingAckSubscriptionKeys.add(subscriptionKey);
    }
    public void removePendingSubscriptionKey(String subscriptionKey) {
        pendingAckSubscriptionKeys.remove(subscriptionKey);
    }
    //-------------------------------------------------------------------------
    public void updateLocation(Location location) {
        if(location == null) {
            throw new NullPointerException("location");
        }

        this.location = location;
    }
    //-------------------------------------------------------------------------
}
