package activemq.kahadb.optimizer.locations;

import org.apache.activemq.store.kahadb.disk.journal.Location;

import static activemq.Utils.isNullOrEmpty;

public final class AckMessageLocation {
    //region private
    private final String subscriptionKey;
    private final Location location;
    //endregion
    public AckMessageLocation(String subscriptionKey, Location location) {
        if(isNullOrEmpty(subscriptionKey)) {
            throw new NullPointerException("subscriptionKey");
        }
        if(location == null) {
            throw new NullPointerException("location");
        }

        this.subscriptionKey = subscriptionKey;
        this.location = location;
    }

    //-------------------------------------------------------------------------
    public String getSubscriptionKey() {
        return subscriptionKey;
    }
    public Location getLocation() {
        return location;
    }
    //-------------------------------------------------------------------------
}
