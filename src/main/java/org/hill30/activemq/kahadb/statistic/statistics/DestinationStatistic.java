package org.hill30.activemq.kahadb.statistic.statistics;

import org.apache.activemq.store.kahadb.data.KahaDestination;
import org.apache.activemq.store.kahadb.data.KahaEntryType;

import javax.management.OperationsException;
import java.util.Iterator;
import java.util.LinkedList;

import static org.hill30.activemq.Utils.isNullOrEmpty;

public final class DestinationStatistic {
    //region private
    private final KahaDestination.DestinationType destinationType;
    private final String destinationId;
    private boolean destinationIdAvailable;
    private final LinkedList<CommandStatistic> commandStatistics = new LinkedList<>();
    //-------------------------------------------------------------------------
    private boolean removed;
    //endregion
    private DestinationStatistic(KahaDestination.DestinationType destinationType, String destinationId) {
        this.destinationId = destinationId;
        this.destinationType = destinationType;

        destinationIdAvailable = this.destinationId != null;
    }
    public DestinationStatistic() {
        this(KahaDestination.DestinationType.QUEUE, null);
    }
    public DestinationStatistic(String destinationId, KahaDestination.DestinationType destinationType) {
        this(destinationType, destinationId);

        if(isNullOrEmpty(destinationId)) {
            throw new NullPointerException("destinationId");
        }
    }

    //-------------------------------------------------------------------------
    public boolean hasDestinationId() {
        return destinationIdAvailable;
    }
    public String getDestinationId() throws OperationsException {
        if(hasDestinationId()) {
            return destinationId;
        }
        throw new OperationsException();
    }
    public KahaDestination.DestinationType getDestinationType() throws OperationsException {
        if(hasDestinationId()) {
            return destinationType;
        }
        throw new OperationsException();
    }
    //-------------------------------------------------------------------------
    public boolean hasRemoved() {
        return removed;
    }
    //-------------------------------------------------------------------------
    public CommandStatistic[] getCommandStatistics() {
        CommandStatistic[] arrayCommandStatistics = new CommandStatistic[commandStatistics.size()];
        return commandStatistics.toArray(arrayCommandStatistics);
    }
    //-------------------------------------------------------------------------
    public void addCommandStatistic(DestinationStatistic source) {
        if(source == null) {
            throw new NullPointerException("source");
        }

        CommandStatistic[] sourceCommandStatistics = source.getCommandStatistics();
        for(CommandStatistic sourceCommandStatistic : sourceCommandStatistics) {
            CommandStatistic commandStatistic = getCommandStatistic(sourceCommandStatistic.getCommandType(), sourceCommandStatistic.hasReversCommand());
            commandStatistic.incrementSize(sourceCommandStatistic);
        }
    }
    public CommandStatistic getCommandStatistic(KahaEntryType commandType, boolean reversCommand) {
        CommandStatistic commandStatistic = null;

        final Iterator<CommandStatistic> iteratorCommandStatistics = commandStatistics.iterator();
        while (iteratorCommandStatistics.hasNext()) {
            CommandStatistic nextCommandStatistic = iteratorCommandStatistics.next();
            if(nextCommandStatistic.getCommandType() == commandType && nextCommandStatistic.hasReversCommand() == reversCommand) {
                commandStatistic = nextCommandStatistic;
                break;
            }
        }

        if(commandStatistic == null) {
            commandStatistic = new CommandStatistic(commandType, reversCommand);
            commandStatistics.add(commandStatistic);
        }

        return commandStatistic;
    }
    //-------------------------------------------------------------------------
    public void setRemoved(boolean removed) {
        if(destinationId != null && !destinationId.isEmpty()) {
            if(!this.removed) {
                this.removed = removed;
            }
        }
    }
    //-------------------------------------------------------------------------

}
