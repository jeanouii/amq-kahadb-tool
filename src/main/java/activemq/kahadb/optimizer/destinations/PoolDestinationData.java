package activemq.kahadb.optimizer.destinations;

import activemq.kahadb.optimizer.locations.MessageLocation;
import activemq.kahadb.optimizer.locations.SubscriptionLocation;
import org.apache.activemq.store.kahadb.JournalCommand;
import org.apache.activemq.store.kahadb.data.*;
import org.apache.activemq.store.kahadb.disk.journal.Location;
import org.apache.activemq.store.kahadb.disk.util.DataByteArrayInputStream;
import org.apache.activemq.util.ByteSequence;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

import static activemq.kahadb.utils.KahaDBUtils.*;

public class PoolDestinationData {
    //region private
    private final LinkedList<DestinationData> topicsDestinationData = new LinkedList<>();
    private final LinkedList<DestinationData> queuesDestinationData = new LinkedList<>();
    //-------------------------------------------------------------------------
    private void commandAnalysis(KahaEntryType commandType, JournalCommand<?> command, Location location) {
        try {
            switch (commandType) {
                case KAHA_ADD_MESSAGE_COMMAND: {
                    commandAnalysis((KahaAddMessageCommand)command, location);
                    break;
                }
                case KAHA_UPDATE_MESSAGE_COMMAND: {
                    commandAnalysis((KahaUpdateMessageCommand)command, location);
                    break;
                }
                case KAHA_REMOVE_MESSAGE_COMMAND: {
                    commandAnalysis((KahaRemoveMessageCommand)command, location);
                    break;
                }
                case KAHA_REMOVE_DESTINATION_COMMAND: {
                    commandAnalysis((KahaRemoveDestinationCommand)command);
                    break;
                }
                case KAHA_SUBSCRIPTION_COMMAND: {
                    commandAnalysis((KahaSubscriptionCommand)command, location);
                    break;
                }
            }
        }
        catch (Throwable throwable) {
            showException(throwable);
        }
    }

    private void commandAnalysis(KahaAddMessageCommand command, Location location) {
        KahaDestination destination = command.getDestination();
        String destinationId = getDestinationId(destination);

        if(isDestinationTopic(destination)) {
            DestinationData destinationData = getDestinationData(topicsDestinationData, destinationId);
            if(destinationData != null) {
                destinationData.addMessageLocation(command.getMessageId(), location);
            }
        }
        else {
            DestinationData destinationData = addDestinationData(queuesDestinationData, destinationId);
            destinationData.addMessageLocation(command.getMessageId(), location);
        }
    }
    private void commandAnalysis(KahaRemoveMessageCommand command, Location location) {
        KahaDestination destination = command.getDestination();
        String destinationId = getDestinationId(destination);

        if(isDestinationTopic(destination)) {
            DestinationData destinationData = getDestinationData(topicsDestinationData, destinationId);
            if(destinationData != null) {
                destinationData.removeMessageLocation(command.getMessageId(), command.getSubscriptionKey(), location);
            }
        }
        else {
            DestinationData destinationData = getDestinationData(queuesDestinationData, destinationId);
            if(destinationData != null) {
                destinationData.removeMessageLocation(command.getMessageId());

                if(!destinationData.hasMessages()) {
                    removeDestinationData(queuesDestinationData, destinationId);
                }
            }
        }
    }
    private void commandAnalysis(KahaRemoveDestinationCommand command) {
        KahaDestination destination = command.getDestination();
        String destinationId = getDestinationId(destination);

        LinkedList<DestinationData> listDestinationData = isDestinationTopic(destination) ? topicsDestinationData : queuesDestinationData;
        removeDestinationData(listDestinationData, destinationId);
    }
    private void commandAnalysis(KahaUpdateMessageCommand command, Location location) {
        KahaAddMessageCommand addCommand = command.getMessage();
        KahaDestination destination = addCommand.getDestination();
        String destinationId = getDestinationId(destination);

        LinkedList<DestinationData> listDestinationData = isDestinationTopic(destination) ? topicsDestinationData : queuesDestinationData;
        DestinationData destinationData = getDestinationData(listDestinationData, destinationId);
        MessageLocation messageLocation = null;

        if(destinationData != null) {
            messageLocation = destinationData.getMessageLocation(addCommand.getMessageId());
            if(messageLocation != null) {
                messageLocation.updateLocation(location);
            }
        }

        if(messageLocation == null) {
            commandAnalysis(addCommand, location);
        }
    }
    private void commandAnalysis(KahaSubscriptionCommand command, Location location) {
        KahaDestination destination = command.getDestination();

        if(isDestinationTopic(destination)) {
            LinkedList<DestinationData> listDestinationData = topicsDestinationData;
            String destinationId = getDestinationId(destination);
            String subscriptionKey = command.getSubscriptionKey();

            if(command.hasSubscriptionInfo()) {
                DestinationData destinationData = addDestinationData(listDestinationData, destinationId);
                destinationData.addSubscriptionLocation(subscriptionKey, location, command.getRetroactive());
            }
            else {
                DestinationData destinationData = getDestinationData(listDestinationData, destinationId);
                if(destinationData != null) {
                    destinationData.removeSubscriptionLocation(subscriptionKey);

                    if(!destinationData.hasSubscriptions()) {
                        removeDestinationData(listDestinationData, destinationId);
                    }
                }
            }
        }
    }
    //-------------------------------------------------------------------------
    private DestinationData addDestinationData(LinkedList<DestinationData> listDestinationData, String destinationId) {
        DestinationData destinationData = getDestinationData(listDestinationData, destinationId);
        if(destinationData == null) {
            destinationData = new DestinationData(destinationId);
            listDestinationData.add(destinationData);
        }
        return destinationData;
    }
    private boolean removeDestinationData(LinkedList<DestinationData> listDestinationData, String destinationId) {
        boolean removed = false;

        final Iterator<DestinationData> iteratorListDestinationData = listDestinationData.iterator();
        while (iteratorListDestinationData.hasNext()) {
            if (iteratorListDestinationData.next().getDestinationId().equals(destinationId)) {
                iteratorListDestinationData.remove();
                removed = true;
                break;
            }
        }

        return removed;
    }
    private DestinationData getDestinationData(LinkedList<DestinationData> listDestinationData, String destinationId) {
        DestinationData destinationData = null;

        final Iterator<DestinationData> iteratorListDestinationData = listDestinationData.iterator();
        while (iteratorListDestinationData.hasNext()) {
            DestinationData nextDestinationData = iteratorListDestinationData.next();
            if (nextDestinationData.getDestinationId().equals(destinationId)) {
                destinationData = nextDestinationData;
                break;
            }
        }

        return destinationData;
    }
    //-------------------------------------------------------------------------
    private SubscriptionLocation[] getSubscriptionLocations(LinkedList<DestinationData> listDestinationData) {
        LinkedList<SubscriptionLocation> listSubscriptionLocations =  new LinkedList<>();

        for (DestinationData destinationData : listDestinationData) {
            for (SubscriptionLocation subscriptionLocation : destinationData.getSubscriptionLocations()) {
                listSubscriptionLocations.add(subscriptionLocation);
            }
        }

        SubscriptionLocation[] arraySubscriptionLocations = new SubscriptionLocation[listSubscriptionLocations.size()];
        return listSubscriptionLocations.toArray(arraySubscriptionLocations);
    }
    private MessageLocation[] getMessageLocations(LinkedList<DestinationData> listDestinationData) {
        LinkedList<MessageLocation> listMessageLocation = new LinkedList<>();

        for (DestinationData destinationData : listDestinationData) {
            for (MessageLocation message : destinationData.getMessageLocations()) {
                listMessageLocation.add(message);
            }
        }

        MessageLocation[] arrayMessageLocation = new MessageLocation[listMessageLocation.size()];
        return listMessageLocation.toArray(arrayMessageLocation);
    }
    //endregion
    //-------------------------------------------------------------------------
    public boolean isEmpty() {
        return getTopicCount() == 0 && getQueueCount() == 0;
    }
    //-------------------------------------------------------------------------
    public int getTopicCount() {
        return topicsDestinationData.size();
    }
    public int getQueueCount() {
        return queuesDestinationData.size();
    }
    //-------------------------------------------------------------------------
    public void sequenceAnalysis(ByteSequence sequence, Location location) throws IOException {
        if(sequence == null) {
            throw new NullPointerException("sequence");
        }
        if(location == null) {
            throw new NullPointerException("location");
        }

        DataByteArrayInputStream sequenceInputStream = new DataByteArrayInputStream(sequence);
        KahaEntryType commandType = KahaEntryType.valueOf(sequenceInputStream.readByte());

        JournalCommand<?> command = (JournalCommand<?>)commandType.createMessage();
        command.mergeFramed(sequenceInputStream);
        //---------------------------------------------------------------------
        commandAnalysis(commandType, command, location);
    }
    //-------------------------------------------------------------------------
    public SubscriptionLocation[] getTopicsSubscriptionLocations() {
        return getSubscriptionLocations(topicsDestinationData);
    }
    //-------------------------------------------------------------------------
    public MessageLocation[] getTopicsMessageLocations() {
        return getMessageLocations(topicsDestinationData);
    }
    public MessageLocation[] getQueuesMessageLocations() {
        return getMessageLocations(queuesDestinationData);
    }
    //-------------------------------------------------------------------------
}
