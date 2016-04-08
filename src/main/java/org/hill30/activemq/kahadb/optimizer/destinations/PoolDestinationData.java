package org.hill30.activemq.kahadb.optimizer.destinations;

import org.hill30.activemq.Utils;
import org.hill30.activemq.kahadb.optimizer.locations.MessageLocation;
import org.hill30.activemq.kahadb.utils.KahaDBTransactionIdConversion;
import org.hill30.activemq.kahadb.optimizer.locations.AckMessageLocation;
import org.hill30.activemq.kahadb.optimizer.locations.SubscriptionLocation;

import org.apache.activemq.command.TransactionId;
import org.apache.activemq.store.kahadb.JournalCommand;
import org.apache.activemq.store.kahadb.data.*;
import org.apache.activemq.store.kahadb.disk.journal.Location;
import org.apache.activemq.store.kahadb.disk.util.DataByteArrayInputStream;
import org.apache.activemq.util.ByteSequence;

import java.io.IOException;
import java.util.*;

import static org.hill30.activemq.kahadb.utils.KahaDBUtils.*;

public class PoolDestinationData {
    //region private
    private final LinkedList<DestinationData> topicsDestinationData = new LinkedList<>();
    private final LinkedList<DestinationData> queuesDestinationData = new LinkedList<>();
    //-------------------------------------------------------------------------
    private final LinkedHashMap<TransactionId, List<OperationLocation>> inflightedTransactions = new LinkedHashMap<>();
    private final LinkedHashMap<TransactionId, TransactionLocation> preparedTransactions = new LinkedHashMap<>();
    private final LinkedHashMap<TransactionId, TransactionLocation> committedTransactionLocations = new LinkedHashMap<>();
    //-------------------------------------------------------------------------
    private void commandAnalysis(JournalCommand<?> command, Location location, boolean transactionAnalysis) {
        KahaEntryType commandType = command.type();
        try {
            switch (commandType) {
                case KAHA_ADD_MESSAGE_COMMAND: {
                    commandAnalysis((KahaAddMessageCommand)command, location, transactionAnalysis);
                    break;
                }
                case KAHA_UPDATE_MESSAGE_COMMAND: {
                    commandAnalysis((KahaUpdateMessageCommand)command, location, transactionAnalysis);
                    break;
                }
                case KAHA_REMOVE_MESSAGE_COMMAND: {
                    commandAnalysis((KahaRemoveMessageCommand)command, location, transactionAnalysis);
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
                case KAHA_COMMIT_COMMAND: {
                    commandAnalysis((KahaCommitCommand)command, location);
                    break;
                }
                case KAHA_PREPARE_COMMAND: {
                    commandAnalysis((KahaPrepareCommand)command, location);
                    break;
                }
                case KAHA_ROLLBACK_COMMAND: {
                    commandAnalysis((KahaRollbackCommand)command);
                }
            }
        }
        catch (Throwable throwable) {
            Utils.showException(throwable);
        }
    }

    private void commandAnalysis(KahaAddMessageCommand command, Location location, boolean transactionAnalysis) {
        if(transactionAnalysis && command.hasTransactionInfo()) {
            List<OperationLocation> inflightTx = getInflightTx(command.getTransactionInfo());
            inflightTx.add(new AddOperationLocation(command, location));
        }
        else {
            KahaDestination destination = command.getDestination();
            String destinationId = getDestinationId(destination);

            if (isDestinationTopic(destination)) {
                DestinationData destinationData = getDestinationData(topicsDestinationData, destinationId);
                if (destinationData != null) {
                    destinationData.addMessageLocation(command.getMessageId(), location);
                }
            }
            else {
                DestinationData destinationData = addDestinationData(queuesDestinationData, destinationId);
                destinationData.addMessageLocation(command.getMessageId(), location);
            }
        }
    }
    private void commandAnalysis(KahaUpdateMessageCommand command, Location location, boolean transactionAnalysis) {
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
            commandAnalysis(addCommand, location, transactionAnalysis);
        }
    }
    private void commandAnalysis(KahaRemoveMessageCommand command, Location location, boolean transactionAnalysis) {
        if(transactionAnalysis && command.hasTransactionInfo()) {
            List<OperationLocation> inflightTx = getInflightTx(command.getTransactionInfo());
            inflightTx.add(new RemoveOperationLocation(command, location));
        }
        else {
            KahaDestination destination = command.getDestination();
            String destinationId = getDestinationId(destination);
            String messageId = command.getMessageId();

            if (isDestinationTopic(destination)) {
                DestinationData destinationData = getDestinationData(topicsDestinationData, destinationId);
                if (destinationData != null) {
                    destinationData.removeMessageLocation(messageId, command.getSubscriptionKey(), location);
                }
            }
            else {
                DestinationData destinationData = getDestinationData(queuesDestinationData, destinationId);
                if (destinationData != null) {
                    destinationData.removeMessageLocation(messageId);

                    if (!destinationData.hasMessages()) {
                        removeDestinationData(queuesDestinationData, destinationId);
                    }
                }
            }

            removeMessageOfCommittedTransaction(messageId);
        }
    }
    private void commandAnalysis(KahaRemoveDestinationCommand command) {
        KahaDestination destination = command.getDestination();
        String destinationId = getDestinationId(destination);

        LinkedList<DestinationData> listDestinationData = isDestinationTopic(destination) ? topicsDestinationData : queuesDestinationData;
        removeDestinationData(listDestinationData, destinationId);
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
    private void commandAnalysis(KahaCommitCommand command, Location location) {
        if(!command.hasTransactionInfo()) {
            return;
        }

        TransactionId transactionId = KahaDBTransactionIdConversion.convert(command.getTransactionInfo());
        List<OperationLocation> inflightTx = inflightedTransactions.remove(transactionId);
        if(inflightTx == null) {
            TransactionLocation prepareMessageLocation = preparedTransactions.remove(transactionId);
            inflightTx = prepareMessageLocation == null ? null : prepareMessageLocation.getOperationLocations();
        }

        if(inflightTx != null) {
            CommittedTransactionLocation committedTransactionLocation = new CommittedTransactionLocation(transactionId, location, inflightTx);
            committedTransactionLocations.put(committedTransactionLocation.getTransactionId(), committedTransactionLocation);

            inflightTx.forEach(OperationLocation::execute);
        }
    }
    private void commandAnalysis(KahaPrepareCommand command, Location location) {
        if(!command.hasTransactionInfo()) {
            return;
        }

        TransactionId transactionId = KahaDBTransactionIdConversion.convert(command.getTransactionInfo());
        List<OperationLocation> inflightTx = inflightedTransactions.remove(transactionId);
        if(inflightTx != null) {
            preparedTransactions.put(transactionId, new TransactionLocation(transactionId, location, inflightTx));
        }
    }
    private void commandAnalysis(KahaRollbackCommand command) {
        if(!command.hasTransactionInfo()) {
            return;
        }

        TransactionId transactionId = KahaDBTransactionIdConversion.convert(command.getTransactionInfo());
        List<OperationLocation> inflightTx = inflightedTransactions.remove(transactionId);
        if(inflightTx == null) {
            preparedTransactions.remove(transactionId);
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
    private void removeMessageOfCommittedTransaction(String messageId) {
        final Iterator<TransactionLocation> iteratorTransactionLocations = committedTransactionLocations.values().iterator();
        while (iteratorTransactionLocations.hasNext()) {
            TransactionLocation nextTransactionLocations = iteratorTransactionLocations.next();
            if(((CommittedTransactionLocation)nextTransactionLocations).removeCommandLocation(messageId)) {
                if(!nextTransactionLocations.hasCommandLocations()) {
                    iteratorTransactionLocations.remove();
                }
            }
        }
    }
    //-------------------------------------------------------------------------
    private List<OperationLocation> getInflightTx(KahaTransactionInfo info) {
        TransactionId transactionId = KahaDBTransactionIdConversion.convert(info);
        List<OperationLocation> inflightTx = inflightedTransactions.get(transactionId);

        if(inflightTx == null) {
            inflightTx = new ArrayList<>();
            inflightedTransactions.put(transactionId, inflightTx);
        }

        return inflightTx;
    }
    //-------------------------------------------------------------------------
    private void addSubscriptionLocations(LinkedList<DestinationData> destinationDates, List<Location> target) {
        for (DestinationData destinationData : destinationDates) {
            for (SubscriptionLocation subscriptionLocation : destinationData.getSubscriptionLocations()) {
                target.add(subscriptionLocation.getLocation());
            }
        }
    }
    private void addDestinationLocations(Collection<DestinationData> destinationDates, List<Location> target) {
        for(DestinationData destinationData : destinationDates) {
            for(MessageLocation messageLocation : destinationData.getMessageLocations()) {
                target.add(messageLocation.getLocation());

                for(AckMessageLocation ackMessageLocation : messageLocation.getAckMessageLocations()) {
                    target.add(ackMessageLocation.getLocation());
                }
            }
        }
    }
    private void addTransactionLocations(Collection<TransactionLocation> transactionLocations, List<Location> target, boolean addCommandLocations) {
        for(TransactionLocation transactionLocation : transactionLocations) {
            target.add(transactionLocation.getLocation());

            if(addCommandLocations) {
                addOperationLocations(transactionLocation.getOperationLocations(), target);
            }
        }
    }
    private void addOperationLocations(Collection<OperationLocation> operationLocationLocations, List<Location> target) {
        for(OperationLocation operationLocation : operationLocationLocations) {
            target.add(operationLocation.getLocation());
        }
    }
    //-------------------------------------------------------------------------
    private abstract class OperationLocation<T extends JournalCommand<T>> {
        //region private
        private String messageId;
        private final T command;
        private final Location location;
        //endregion
        OperationLocation(String messageId, T command, Location location) {
            this.messageId = messageId;
            this.command = command;
            this.location = location;
        }

        //-------------------------------------------------------------------------
        public String getMessageId() {
            return messageId;
        }
        public T getCommand() {
            return command;
        }
        public Location getLocation() {
            return location;
        }
        //-------------------------------------------------------------------------
        public void execute() {
            commandAnalysis(command, location, false);
        }
        //-------------------------------------------------------------------------
    }
    private final class AddOperationLocation extends OperationLocation<KahaAddMessageCommand> {
        AddOperationLocation(KahaAddMessageCommand command, Location location) {
            super(command.getMessageId(), command, location);
        }
    }
    private final class RemoveOperationLocation extends OperationLocation<KahaRemoveMessageCommand> {
        RemoveOperationLocation(KahaRemoveMessageCommand command, Location location) {
            super(command.getMessageId(), command, location);
        }
    }
    //-------------------------------------------------------------------------
    private class TransactionLocation {
        //region private
        private final TransactionId transactionId;
        private final Location location;
        private final List<OperationLocation> operationLocations;
        //endregion
        TransactionLocation(TransactionId transactionId, Location location, List<OperationLocation> operationLocations) {
            this.transactionId = transactionId;
            this.location = location;
            this.operationLocations = operationLocations;
        }

        //-------------------------------------------------------------------------
        public TransactionId getTransactionId() {
            return transactionId;
        }
        public Location getLocation() {
            return location;
        }
        public boolean hasCommandLocations() {
            return operationLocations.size() != 0;
        }
        //---------------------------------------------------------------------
        List<OperationLocation> getOperationLocations() {
            return operationLocations;
        }
        //---------------------------------------------------------------------
    }
    private class CommittedTransactionLocation extends TransactionLocation {
        CommittedTransactionLocation(TransactionId transactionId, Location location, List<OperationLocation> operationLocations) {
            super(transactionId, location, new ArrayList<>(operationLocations));
        }

        //---------------------------------------------------------------------
        boolean removeCommandLocation(String messageId) {
            boolean removed = false;

            final Iterator<OperationLocation> iteratorCommandLocations = getOperationLocations().iterator();
            while (iteratorCommandLocations.hasNext()) {
                OperationLocation operationLocation = iteratorCommandLocations.next();
                if(operationLocation.getMessageId().equals(messageId)) {
                    iteratorCommandLocations.remove();
                    removed = true;
                }
            }

            return removed;
        }
        //---------------------------------------------------------------------
    }
    //-------------------------------------------------------------------------
    private final class LocationComparator implements Comparator<Location> {
        @Override
        public int compare(Location o1, Location o2) {
            return o1.compareTo(o2);
        }
    }
    //endregion
    //-------------------------------------------------------------------------
    public boolean isEmpty() {
        return getTopicCount() == 0
                && getQueueCount() == 0
                && getPreparedTransactionCount() == 0
                && getCommittedTransactionCount() == 0;
    }
    //-------------------------------------------------------------------------
    public int getTopicCount() {
        return topicsDestinationData.size();
    }
    public int getQueueCount() {
        return queuesDestinationData.size();
    }
    public int getPreparedTransactionCount() {
        return preparedTransactions.size();
    }
    public int getCommittedTransactionCount() {
        return committedTransactionLocations.size();
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
        commandAnalysis(command, location, true);
    }
    public void gc() {
        //discard any inflighted transactions
        inflightedTransactions.clear();
    }
    //-------------------------------------------------------------------------
    public Location[] getSubscriptionLocations() {
        List<Location> locations = new ArrayList<>();

        addSubscriptionLocations(topicsDestinationData, locations);

        locations.sort(new LocationComparator());

        Location[] arrayLocation = new Location[locations.size()];
        return locations.toArray(arrayLocation);
    }
    public Location[] getMessageLocations() {
        List<Location> locations = new ArrayList<>();

        addDestinationLocations(topicsDestinationData, locations);
        addDestinationLocations(queuesDestinationData, locations);
        addTransactionLocations(committedTransactionLocations.values(), locations, false);
        addTransactionLocations(preparedTransactions.values(), locations, true);

        if(committedTransactionLocations.size() != 0 || preparedTransactions.size() != 0) {
            locations.sort(new LocationComparator());
        }

        Location[] arrayLocation = new Location[locations.size()];
        return locations.toArray(arrayLocation);
    }
    //-------------------------------------------------------------------------
}
