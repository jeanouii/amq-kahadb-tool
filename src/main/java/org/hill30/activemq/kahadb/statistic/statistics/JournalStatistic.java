package org.hill30.activemq.kahadb.statistic.statistics;

import org.apache.activemq.store.kahadb.JournalCommand;
import org.apache.activemq.store.kahadb.data.KahaAddMessageCommand;
import org.apache.activemq.store.kahadb.data.KahaDestination;
import org.apache.activemq.store.kahadb.data.KahaEntryType;
import org.apache.activemq.store.kahadb.data.KahaRemoveDestinationCommand;
import org.apache.activemq.store.kahadb.data.KahaRemoveMessageCommand;
import org.apache.activemq.store.kahadb.data.KahaSubscriptionCommand;
import org.apache.activemq.store.kahadb.data.KahaUpdateMessageCommand;
import org.apache.activemq.store.kahadb.disk.util.DataByteArrayInputStream;
import org.apache.activemq.util.ByteSequence;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

import static org.hill30.activemq.Utils.showException;
import static org.hill30.activemq.kahadb.utils.KahaDBUtils.getDestinationId;
import static org.hill30.activemq.kahadb.utils.KahaDBUtils.isDestinationTopic;

public final class JournalStatistic {
    //region private
    private final File file;
    private final LinkedList<DestinationStatistic> topicsDestinationStatistics = new LinkedList<>();
    private final LinkedList<DestinationStatistic> queuesDestinationStatistics = new LinkedList<>();
    private final LinkedList<DestinationStatistic> otherDestinationStatistics = new LinkedList<>();
    //-------------------------------------------------------------------------
    private void addCommand(KahaEntryType commandType, JournalCommand<?> command, long size) {
        KahaDestination destination = getCommandDestination(commandType, command);
        boolean isReversCommand = isReversCommand(commandType, command);
        DestinationStatistic destinationStatistic;

        if(destination == null) {
            destinationStatistic = getDestinationStatistic(otherDestinationStatistics, destination);
        }
        else {
            destinationStatistic = isDestinationTopic(destination)
                    ? getDestinationStatistic(topicsDestinationStatistics, destination)
                    : getDestinationStatistic(queuesDestinationStatistics, destination);
        }

        CommandStatistic commandStatistic = destinationStatistic.getCommandStatistic(commandType, isReversCommand);
        commandStatistic.incrementSize(size);

        if(isRemoveDestination(commandType)) {
            destinationStatistic.setRemoved(true);
        }
    }
    //-------------------------------------------------------------------------
    private KahaDestination getCommandDestination(KahaEntryType commandType, JournalCommand<?> command) {
        switch (commandType) {
            case KAHA_ADD_MESSAGE_COMMAND: {
                return ((KahaAddMessageCommand)command).getDestination();
            }
            case KAHA_UPDATE_MESSAGE_COMMAND: {
                return ((KahaUpdateMessageCommand)command).getMessage().getDestination();
            }
            case KAHA_REMOVE_MESSAGE_COMMAND: {
                return ((KahaRemoveMessageCommand)command).getDestination();
            }
            case KAHA_REMOVE_DESTINATION_COMMAND: {
                return ((KahaRemoveDestinationCommand)command).getDestination();
            }
            case KAHA_SUBSCRIPTION_COMMAND: {
                return ((KahaSubscriptionCommand)command).getDestination();
            }
            default: {
                return null;
            }
        }
    }
    //-------------------------------------------------------------------------
    private boolean isRemoveDestination(KahaEntryType commandType) {
        return commandType == KahaEntryType.KAHA_REMOVE_DESTINATION_COMMAND;
    }
    private boolean isReversCommand(KahaEntryType commandType, JournalCommand<?> command) {
        if(commandType == KahaEntryType.KAHA_SUBSCRIPTION_COMMAND) {
            return !((KahaSubscriptionCommand)command).hasSubscriptionInfo();
        }
        return false;
    }
    //-------------------------------------------------------------------------
    private DestinationStatistic getDestinationStatistic(LinkedList<DestinationStatistic> listDestinationStatistics, KahaDestination destination) {
        DestinationStatistic destinationStatistic = null;
        boolean isDestinationAvailable = destination != null;
        String destinationId = isDestinationAvailable ? getDestinationId(destination) : "";

        final Iterator<DestinationStatistic> iteratorDestinationStatistics = listDestinationStatistics.iterator();
        while (iteratorDestinationStatistics.hasNext()) {
            DestinationStatistic nextDestinationStatistic = iteratorDestinationStatistics.next();
            if(isDestinationAvailable) {
                if(nextDestinationStatistic.hasDestinationId()) {
                    try {
                        if (nextDestinationStatistic.getDestinationId().equals(destinationId) && !nextDestinationStatistic.hasRemoved()) {
                            destinationStatistic = nextDestinationStatistic;
                            break;
                        }
                    } catch (Throwable throwable) {
                        showException(throwable);
                    }
                }
            }
            else {
                if(!nextDestinationStatistic.hasDestinationId()) {
                    destinationStatistic = nextDestinationStatistic;
                    break;
                }
            }


        }

        if(destinationStatistic == null) {
            destinationStatistic = isDestinationAvailable ? new DestinationStatistic(destinationId, destination.getType()) : new DestinationStatistic();
            listDestinationStatistics.add(destinationStatistic);
        }

        return destinationStatistic;
    }
    //-------------------------------------------------------------------------
    private void addCommandStatistics(LinkedList<DestinationStatistic> listSourceDestinationStatistics, DestinationStatistic target) {
        listSourceDestinationStatistics.forEach(target::addCommandStatistic);
    }
    //-------------------------------------------------------------------------
    private int getMessagesCount(LinkedList<DestinationStatistic> listDestinationStatistics) {
        int messagesCount = 0;
        for (DestinationStatistic destinationStatistic : listDestinationStatistics) {
            for (CommandStatistic commandStatistic : destinationStatistic.getCommandStatistics()) {
                messagesCount += commandStatistic.getCount();
            }
        }
        return messagesCount;
    }
    private int getMessagesCount(LinkedList<DestinationStatistic> listDestinationStatistics, KahaEntryType commandType, boolean reversCommand) {
        int messagesCount = 0;
        for (DestinationStatistic destinationStatistic : listDestinationStatistics) {
            for (CommandStatistic commandStatistic : destinationStatistic.getCommandStatistics()) {
                if(commandStatistic.getCommandType() == commandType && commandStatistic.hasReversCommand() == reversCommand) {
                    messagesCount += commandStatistic.getCount();
                }
            }
        }
        return messagesCount;
    }
    //endregion
    public JournalStatistic(File file) {
        if(file == null) {
            throw new NullPointerException("file");
        }

        this.file = file;
    }

    //-------------------------------------------------------------------------
    public File getFile() {
        return file;
    }
    public boolean hasStatistic() {
        return topicsDestinationStatistics.size() != 0 || queuesDestinationStatistics.size() != 0 || otherDestinationStatistics.size() != 0;
    }
    //-------------------------------------------------------------------------
    public int getTopicCount() {
        return topicsDestinationStatistics.size();
    }
    public int getQueueCount() {
        return queuesDestinationStatistics.size();
    }
    //-------------------------------------------------------------------------
    public int getTopicsMessagesCount() {
        return getMessagesCount(topicsDestinationStatistics);
    }
    public int getQueuesMessagesCount() {
        return getMessagesCount(queuesDestinationStatistics);
    }
    public int getOtherMessagesCount() {
        return getMessagesCount(otherDestinationStatistics);
    }
    //-------------------------------------------------------------------------
    public int getTopicsSubscriptionsCount() {
        return getMessagesCount(topicsDestinationStatistics, KahaEntryType.KAHA_SUBSCRIPTION_COMMAND, false);
    }
    public int getTopicsUnsubscriptionsCount() {
        return getMessagesCount(topicsDestinationStatistics, KahaEntryType.KAHA_SUBSCRIPTION_COMMAND, true);
    }
    //-------------------------------------------------------------------------
    public DestinationStatistic[] getTopicsDestinationStatistics() {
        DestinationStatistic[] arrayDestinationStatistics = new DestinationStatistic[topicsDestinationStatistics.size()];
        return topicsDestinationStatistics.toArray(arrayDestinationStatistics);
    }
    public DestinationStatistic[] getQueueDestinationStatistics() {
        DestinationStatistic[] arrayDestinationStatistics = new DestinationStatistic[queuesDestinationStatistics.size()];
        return queuesDestinationStatistics.toArray(arrayDestinationStatistics);
    }
    public DestinationStatistic[] getOtherDestinationStatistics() {
        DestinationStatistic[] arrayDestinationStatistics = new DestinationStatistic[otherDestinationStatistics.size()];
        return otherDestinationStatistics.toArray(arrayDestinationStatistics);
    }
    //-------------------------------------------------------------------------
    public CommandStatistic[] getCommandStatistics() {
        DestinationStatistic destinationStatistic = new DestinationStatistic();

        addCommandStatistics(topicsDestinationStatistics, destinationStatistic);
        addCommandStatistics(queuesDestinationStatistics, destinationStatistic);
        addCommandStatistics(otherDestinationStatistics, destinationStatistic);

        return destinationStatistic.getCommandStatistics();
    }
    //-------------------------------------------------------------------------
    public void addSequence(ByteSequence sequence, long size) throws IOException {
        if(sequence == null) {
            throw new NullPointerException("sequence");
        }
        if(size < 0) {
            throw new IndexOutOfBoundsException("size");
        }

        DataByteArrayInputStream sequenceInputStream = new DataByteArrayInputStream(sequence);
        KahaEntryType commandType = KahaEntryType.valueOf(sequenceInputStream.readByte());

        JournalCommand<?> command = (JournalCommand<?>)commandType.createMessage();
        command.mergeFramed(sequenceInputStream);

        addCommand(commandType, command, size);
    }
    //-------------------------------------------------------------------------
}
