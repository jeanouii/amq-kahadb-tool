package activemq.kahadb.statistic;

import activemq.kahadb.statistic.statistics.CommandStatistic;
import activemq.kahadb.statistic.statistics.DestinationStatistic;
import activemq.kahadb.statistic.statistics.JournalStatistic;
import org.apache.activemq.store.kahadb.disk.journal.Journal;
import org.apache.activemq.store.kahadb.disk.journal.Location;
import org.apache.activemq.util.ByteSequence;

import javax.management.OperationsException;
import java.io.File;
import java.io.IOException;

import static activemq.kahadb.utils.KahaDBUtils.*;

public class KahaDBJournalsStatistics {
    //region private
    private final String sourceDirPath;
    //-------------------------------------------------------------------------
    private Journal sourceJournal;
    private boolean useContinue;
    //-------------------------------------------------------------------------
    private void createJournals() {
        sourceJournal = createJournal(new File(sourceDirPath));
    }
    //-------------------------------------------------------------------------
    private void showJournalsStatistic() {
        try {
            sourceJournal.start();

            int fileIndex = 0;
            JournalStatistic lastJournalStatistic = null;
            long headerSize = -1;
            Location location = sourceJournal.getNextLocation(null);
            while (location != null) {
                File nextFile = sourceJournal.getFile(location.getDataFileId());
                if(lastJournalStatistic == null || !lastJournalStatistic.getFile().equals(nextFile)) {
                    if(lastJournalStatistic != null) {
                        showJournalStatistic(fileIndex, lastJournalStatistic);
                    }
                    else {
                        showSeparator();
                    }

                    if(useContinue && lastJournalStatistic != null) {
                        pressAnyKeyToContinue();
                        showSeparator();
                    }
                    ++fileIndex;
                    lastJournalStatistic = new JournalStatistic(nextFile);
                }

                if(headerSize == -1) {
                    headerSize = location.getOffset();
                }
                ByteSequence sequence = sourceJournal.read(location);
                lastJournalStatistic.addSequence(sequence, location.getSize() + headerSize);

                location = sourceJournal.getNextLocation(location);
            }

            if(lastJournalStatistic != null) {
                showJournalStatistic(fileIndex, lastJournalStatistic);
            }
        }
        catch (Throwable throwable) {
            showException(throwable);
        }
        finally {
            try {
                sourceJournal.close();
            }
            catch (IOException e) {}
        }
    }
    private void showJournalStatistic(int fileIndex, JournalStatistic journalStatistic) throws OperationsException {
        System.out.printf("(%s) Journal: '%s'.\r\n", fileIndex, journalStatistic.getFile());

        if(journalStatistic.hasStatistic()) {
            showJournalDestinationStatistics(journalStatistic);
            showJournalCommandStatistics(journalStatistic);
        }

        showSeparator();
    }

    private void showJournalDestinationStatistics(JournalStatistic journalStatistic) throws OperationsException {
        showSeparator();
        System.out.printf("Destinations statistics:\r\n");
        System.out.printf("- Topics: %s.\r\n", journalStatistic.getTopicCount());
        System.out.printf("- Queues: %s.\r\n", journalStatistic.getQueueCount());
        showSeparator(false);

        showCommandStatistics(journalStatistic.getTopicsDestinationStatistics());
        showCommandStatistics(journalStatistic.getQueueDestinationStatistics());

        DestinationStatistic[] otherDestinationStatistics = journalStatistic.getOtherDestinationStatistics();
        if(otherDestinationStatistics.length != 0) {
            System.out.println();
            System.out.printf("Other commands:\r\n");
            showCommandStatistics(otherDestinationStatistics);
        }
    }
    private void showJournalCommandStatistics(JournalStatistic journalStatistic) {
        CommandStatistic[] commandStatistics = journalStatistic.getCommandStatistics();

        showSeparator();
        System.out.printf("Commands statistics:\r\n");
        showSeparator(true);

        showTopicsCommandStatistics(journalStatistic);
        showQueuesCommandStatistics(journalStatistic);
        showOtherCommandStatistics(journalStatistic);
        System.out.println();

        showCommandStatistics(commandStatistics);
    }

    private void showCommandStatistics(DestinationStatistic[] destinationStatistics) throws OperationsException {
        for (DestinationStatistic destinationStatistic : destinationStatistics) {
            CommandStatistic[] commandStatistics = destinationStatistic.getCommandStatistics();
            if(destinationStatistic.hasDestinationId()) {
                System.out.println();

                String destinationRemovedStr = destinationStatistic.hasRemoved() ? "- " : "";
                String destinationInfo = getDestinationInfo(destinationStatistic.getDestinationType(), destinationStatistic.getDestinationId());
                System.out.printf("%s%s.\r\n", destinationRemovedStr, destinationInfo);
            }
            showCommandStatistics(commandStatistics);
        }
    }
    private void showCommandStatistics(CommandStatistic[] commandStatistics) {
        long commandsTotalSize = 0;
        int allCommand = 0;

        for (CommandStatistic commandStatistic : commandStatistics) {
            String message = commandStatistic.hasReversCommand() ? "-" : "+";
            long totalSize = commandStatistic.getTotalSize();
            commandsTotalSize += totalSize;
            allCommand += commandStatistic.getCount();

            message += " CmdType: " + commandStatistic.getCommandType()
                    + " (Count: " + commandStatistic.getCount()
                    + ", TotalSize: " + bytesToString(totalSize)
                    + ", ~AvrgSize: " + bytesToString(commandStatistic.getAvrgSize())
                    + ", LastBigSize: " + bytesToString(commandStatistic.getLastBigSize())
                    + ", LastSize: " + bytesToString(commandStatistic.getLastSize())
                    + ")";
            System.out.println(message);
        }

        if(commandsTotalSize != 0) {
            System.out.printf("All commands: %s (Total size: %s.\r\n", allCommand, bytesToString(commandsTotalSize));
        }
    }
    //-------------------------------------------------------------------------
    private void showTopicsCommandStatistics(JournalStatistic journalStatistic) {
        System.out.printf("- Topics: %s (messages: %s, +subscriptions: %s, -subscription: %s).\r\n",
                journalStatistic.getTopicCount(),
                journalStatistic.getTopicsMessagesCount(),
                journalStatistic.getTopicsSubscriptionsCount(),
                journalStatistic.getTopicsUnsubscriptionsCount()
        );
    }
    private void showQueuesCommandStatistics(JournalStatistic journalStatistic) {
        System.out.printf("- Queues: %s (messages: %s).\r\n",
                journalStatistic.getQueueCount(),
                journalStatistic.getQueuesMessagesCount()
        );
    }
    private void showOtherCommandStatistics(JournalStatistic journalStatistic) {
        System.out.printf("- Other messages: %s.\r\n",
                journalStatistic.getOtherMessagesCount()
        );
    }
    //endregion
    public KahaDBJournalsStatistics(String sourceDirPath) {
        if(isNullOrEmpty(sourceDirPath)) {
            throw new NullPointerException("sourceDirPath");
        }

        this.sourceDirPath = sourceDirPath;
    }

    //-------------------------------------------------------------------------
    public void showStatistic(boolean useContinue) {
        this.useContinue = useContinue;

        createJournals();
        showJournalsStatistic();
    }
    //-------------------------------------------------------------------------
}
