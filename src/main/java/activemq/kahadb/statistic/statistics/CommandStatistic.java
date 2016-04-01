package activemq.kahadb.statistic.statistics;

import org.apache.activemq.store.kahadb.data.KahaEntryType;

public class CommandStatistic {
    //region private
    private final KahaEntryType commandType;
    private final boolean reversCommand;
    //-------------------------------------------------------------------------
    private int count;
    private long totalSize;
    private long lastBigSize;
    private long lastSize;
    //endregion
    public CommandStatistic(KahaEntryType commandType, boolean reversCommand) {
        this.commandType = commandType;
        this.reversCommand = reversCommand;
    }

    //-------------------------------------------------------------------------
    public KahaEntryType getCommandType() {
        return commandType;
    }
    public boolean hasReversCommand() {
        return reversCommand;
    }
    //-------------------------------------------------------------------------
    public int getCount() {
        return count;
    }
    public long getTotalSize() {
        return totalSize;
    }
    public long getLastBigSize() {
        return lastBigSize;
    }
    public long getLastSize() {
        return lastSize;
    }
    public long getAvrgSize() {
        return count == 0 ? 0 : totalSize / count;
    }
    //-------------------------------------------------------------------------
    public void incrementSize(CommandStatistic commandStatistic) {
        if(commandStatistic == null) {
            throw new NullPointerException("commandStatistic");
        }

        count += commandStatistic.count;
        lastSize = commandStatistic.lastSize;
        totalSize += commandStatistic.totalSize;

        if(lastBigSize < commandStatistic.lastBigSize) {
            lastBigSize = commandStatistic.lastBigSize;
        }
    }
    public void incrementSize(long size) {
        ++count;
        lastSize = size;
        totalSize += size;

        if(lastBigSize < lastSize) {
            lastBigSize = lastSize;
        }
    }
    //-------------------------------------------------------------------------
}
