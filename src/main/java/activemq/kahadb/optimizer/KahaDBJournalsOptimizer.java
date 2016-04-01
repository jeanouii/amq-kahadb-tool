package activemq.kahadb.optimizer;

import activemq.kahadb.optimizer.locations.AckMessageLocation;
import activemq.kahadb.optimizer.destinations.PoolDestinationData;
import activemq.kahadb.optimizer.locations.MessageLocation;
import activemq.kahadb.optimizer.locations.SubscriptionLocation;
import org.apache.activemq.store.kahadb.disk.journal.Journal;
import org.apache.activemq.store.kahadb.disk.journal.Location;
import org.apache.activemq.util.ByteSequence;

import java.io.File;
import java.io.IOException;

import static activemq.kahadb.utils.KahaDBUtils.*;

public class KahaDBJournalsOptimizer {
    //region private
    private final PoolDestinationData poolDestinationData = new PoolDestinationData();
    //-------------------------------------------------------------------------
    private final String sourceDirPath;
    private final String targetDirPath;
    //-------------------------------------------------------------------------
    private Journal sourceJournal;
    private Journal targetJournal;
    //-------------------------------------------------------------------------
    private void createJournals() {
        System.out.println("INITIALIZE");

        File sourceDir = new File(sourceDirPath);
        File targetDir = new File(targetDirPath);

        deleteDir(targetDir);
        targetDir.mkdirs();

        int journalSize = getJournalSize(sourceDir);
        System.out.printf("- Journal size: %s.\r\n", bytesToString(journalSize));

        sourceJournal = createJournal(sourceDir, journalSize);
        targetJournal = createJournal(targetDir, journalSize);
    }
    //-------------------------------------------------------------------------
    private void analysis() throws IOException {
        System.out.println("START JOURNALS ANALYSIS");
        System.out.printf("- Directory: '%s'.\r\n", sourceJournal.getDirectory().getCanonicalPath());

        try {
            long start = System.currentTimeMillis();
            sourceJournal.start();

            Location location = sourceJournal.getNextLocation(null);
            while (location != null) {
                ByteSequence sequence = sourceJournal.read(location);
                poolDestinationData.sequenceAnalysis(sequence, location);
                location = sourceJournal.getNextLocation(location);
            }

            long end = System.currentTimeMillis();
            System.out.printf("- Journals: %s (Total size: %s).\n\r", sourceJournal.getFiles().size(), bytesToString(sourceJournal.getDiskSize()));
            System.out.printf("- Topics: %s.\n\r", poolDestinationData.getTopicCount());
            System.out.printf("- Queues: %s.\n\r", poolDestinationData.getQueueCount());
            System.out.printf("- It took time: %s seconds.\r\n", ((end - start) / 1000.0f));
        }
        catch (Throwable throwable) {
            showException(throwable);
        }
        finally {
            try {
                sourceJournal.close();
            } catch (IOException e) {  }
        }
    }

    private void dataOptimization() {
        System.out.println("START JOURNALS DATA OPTIMIZATION");

        try {
            long start = System.currentTimeMillis();

            targetJournal.start();

            topicsSubscriptionsMove();
            topicsMessagesMove();
            queuesMessagesMove();

            long end = System.currentTimeMillis();
            System.out.println();
            System.out.printf("- Journals remained: %s (Total size: %s).\n\r", targetJournal.getFiles().size(), bytesToString(targetJournal.getDiskSize()));
            System.out.printf("- It took time: %s seconds.\r\n", ((end - start) / 1000.0f));
        }
        catch (Throwable throwable) {
            showException(throwable);
        }
        finally {
            try {
                targetJournal.close();
            } catch (IOException e) {  }
        }
    }
    private void topicsSubscriptionsMove() {
        try {
            long start = System.currentTimeMillis();

            SubscriptionLocation[] topicsSubscriptionLocations = poolDestinationData.getTopicsSubscriptionLocations();
            sourceJournal.start();

            for (SubscriptionLocation subscriptionLocation : topicsSubscriptionLocations) {
                ByteSequence sequence = sourceJournal.read(subscriptionLocation.getLocation());
                targetJournal.write(sequence, true);
            }

            long end = System.currentTimeMillis();
            System.out.printf("- Topics subscriptions moved: %s (It took time: %s seconds).\r\n", topicsSubscriptionLocations.length, ((end - start) / 1000.0f));
        }
        catch (Throwable throwable) {
            showException(throwable);
        }
        finally {
            try {
                sourceJournal.close();
            } catch (IOException e) { }
        }
    }
    private void topicsMessagesMove() {
        try {
            long start = System.currentTimeMillis();

            MessageLocation[] topicsMessageLocations = poolDestinationData.getTopicsMessageLocations();
            sourceJournal.start();

            int messagesCounter = topicsMessageLocations.length;
            for(MessageLocation messageLocation : topicsMessageLocations) {
                ByteSequence sequence = sourceJournal.read(messageLocation.getLocation());
                targetJournal.write(sequence, true);

                AckMessageLocation[] ackMessageLocations = messageLocation.getAckMessageLocations();
                messagesCounter += ackMessageLocations.length;

                for(AckMessageLocation ackMessageLocation : ackMessageLocations) {
                    sequence = sourceJournal.read(ackMessageLocation.getLocation());
                    targetJournal.write(sequence, true);
                }
            }

            long end = System.currentTimeMillis();
            System.out.printf("- Topics messages moved: %s (It took time: %s seconds).\r\n", messagesCounter, ((end - start) / 1000.0f));
        }
        catch (Throwable throwable) {
            showException(throwable);
        }
        finally {
            try {
                sourceJournal.close();
            } catch (IOException e) {  }
        }
    }
    private void queuesMessagesMove() {
        try {
            long start = System.currentTimeMillis();

            MessageLocation[] queuesMessageLocations = poolDestinationData.getQueuesMessageLocations();
            sourceJournal.start();

            int messagesCounter = queuesMessageLocations.length;
            for(MessageLocation messageLocation : queuesMessageLocations) {
                ByteSequence sequence = sourceJournal.read(messageLocation.getLocation());
                targetJournal.write(sequence, true);
            }

            long end = System.currentTimeMillis();
            System.out.printf("- Queues messages moved: %s (It took time: %s seconds).\r\n", messagesCounter, ((end - start) / 1000.0f));
        }
        catch (Throwable throwable) {
            showException(throwable);
        }
        finally {
            try {
                sourceJournal.close();
            } catch (IOException e) {  }
        }
    }

    private void renameDirs() {
        System.out.println("RENAME JOURNALS DIRECTORIES");

        File sourceDir = sourceJournal.getDirectory();
        File targetDir = targetJournal.getDirectory();

        try {
            String sourceDirPath = sourceDir.getCanonicalPath();
            String newSourceDirPath = sourceDirPath + "_" + (System.currentTimeMillis());

            File newSourceDir = new File(newSourceDirPath);
            newSourceDir.deleteOnExit();

            sourceDir.renameTo(newSourceDir);
            targetDir.renameTo(new File(sourceDirPath));

            System.out.printf("- Directory optimized: '%s'.\r\n", sourceDirPath);
            System.out.printf("- Backup directory: '%s'.\r\n", newSourceDirPath);

        } catch (Throwable throwable) {
            showException(throwable);
        }
    }
    //endregion
    public KahaDBJournalsOptimizer(String sourceDirPath) {
        if(isNullOrEmpty(sourceDirPath)) {
            throw new NullPointerException("sourceDirPath");
        }

        File sourceDir = new File(sourceDirPath);
        if(!sourceDir.exists() || !sourceDir.isDirectory()) {
            throw new IllegalArgumentException("sourceDirPath");
        }

        this.sourceDirPath = sourceDirPath;
        this.targetDirPath = sourceDirPath + "_temp";
    }

    //-------------------------------------------------------------------------
    public void optimaze() throws IOException {
        showSeparator();
        createJournals();

        showSeparator();
        analysis();

        showSeparator();
        dataOptimization();

        showSeparator();
        renameDirs();

        showSeparator();
    }
    //-------------------------------------------------------------------------
}
