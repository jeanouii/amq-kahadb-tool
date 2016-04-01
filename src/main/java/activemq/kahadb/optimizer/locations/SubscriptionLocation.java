package activemq.kahadb.optimizer.locations;

import org.apache.activemq.store.kahadb.disk.journal.Location;

import static activemq.kahadb.utils.KahaDBUtils.isNullOrEmpty;

public class SubscriptionLocation {
    //region private
    private final String subscriptionKey;
    private final String destinationId;
    private final Location location;
    //endregion
    public SubscriptionLocation(String subscriptionKey, String destinationId, Location location) {
        if(isNullOrEmpty(subscriptionKey)) {
            throw new NullPointerException("subscriptionKey");
        }
        if(isNullOrEmpty(destinationId)) {
            throw new NullPointerException("destinationId");
        }
        if(location == null) {
            throw new NullPointerException("location");
        }

        this.subscriptionKey = subscriptionKey;
        this.destinationId = destinationId;
        this.location = location;
    }

    //-------------------------------------------------------------------------
    public String getSubscriptionKey() {
        return subscriptionKey;
    }
    public String getDestinationId() {
        return destinationId;
    }
    public Location getLocation() {
        return location;
    }
    //-------------------------------------------------------------------------
}
