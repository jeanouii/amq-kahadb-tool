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
    private class Journals {
        //region private
        private final Journal sourceJournal;
        private final Journal targetJournal;
        //endregion
        Journals(File sourceDir, File targetDir, int journalSize) {
            this(createJournal(sourceDir, journalSize), createJournal(targetDir, journalSize));
        }
        Journals(Journal sourceJournal, Journal targetJournal) {
            this.sourceJournal = sourceJournal;
            this.targetJournal = targetJournal;
        }

        //---------------------------------------------------------------------
        Journal getSourceJournal() {
            return sourceJournal;
        }
        Journal getTargetJournal() {
            return targetJournal;
        }
        //---------------------------------------------------------------------
    }
    //-------------------------------------------------------------------------
    private void optimaze(File sourceDir, File targetDir, boolean useAnyKeyToContinue) throws IOException {
        showSeparator(2);

        Journals journals = createJournals(sourceDir, targetDir);
        Journal sourceJournal = journals.getSourceJournal();

        PoolDestinationData sourcePoolDestinationData = journalsAnalysis(sourceJournal);

        if(!sourcePoolDestinationData.isEmpty()) {
            showSeparator();
            dataOptimization(sourceJournal, sourcePoolDestinationData, journals.getTargetJournal());
        }
        //---------------------------------------------------------------------
        for (File sourceSubDir : sourceDir.listFiles()) {
            if(sourceSubDir.isDirectory()) {
                if(useAnyKeyToContinue) {
                    showSeparator();
                    pressAnyKeyToContinue();
                }
                String targetSubDirPath = targetDir.getPath() + File.separator +  sourceSubDir.getName();
                optimaze(sourceSubDir, new File(targetSubDirPath), useAnyKeyToContinue);
            }
        }
        //---------------------------------------------------------------------
    }
    //-------------------------------------------------------------------------
    private Journals createJournals(File sourceDir, File targetDir) {
        deleteDir(targetDir);
        targetDir.mkdirs();

        int journalSize = getJournalSize(sourceDir);
        return new Journals(sourceDir, targetDir, journalSize);
    }
    //-------------------------------------------------------------------------
    private PoolDestinationData journalsAnalysis(Journal sourceJournal) {
        final PoolDestinationData poolDestinationData = new PoolDestinationData();

        System.out.println("START JOURNALS ANALYSIS");
        System.out.printf("- Directory: '%s'.\r\n", sourceJournal.getDirectory().getPath());
        System.out.printf("- Journal size: %s.\r\n", bytesToString(sourceJournal.getMaxFileLength()));
        System.out.println();

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

        return poolDestinationData;
    }
    //-------------------------------------------------------------------------
    private void dataOptimization(Journal sourceJournal, PoolDestinationData sourcePoolDestinationData, Journal targetJournal) {
        System.out.println("START JOURNALS DATA OPTIMIZATION");

        try {
            long start = System.currentTimeMillis();

            targetJournal.start();

            topicsSubscriptionsMove(sourceJournal, sourcePoolDestinationData, targetJournal);
            topicsMessagesMove(sourceJournal, sourcePoolDestinationData, targetJournal);
            queuesMessagesMove(sourceJournal, sourcePoolDestinationData, targetJournal);

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
    private void topicsSubscriptionsMove(Journal sourceJournal, PoolDestinationData sourcePoolDestinationData, Journal targetJournal) {
        try {
            long start = System.currentTimeMillis();

            SubscriptionLocation[] topicsSubscriptionLocations = sourcePoolDestinationData.getTopicsSubscriptionLocations();
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
    private void topicsMessagesMove(Journal sourceJournal, PoolDestinationData sourcePoolDestinationData, Journal targetJournal) {
        try {
            long start = System.currentTimeMillis();

            MessageLocation[] topicsMessageLocations = sourcePoolDestinationData.getTopicsMessageLocations();
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
    private void queuesMessagesMove(Journal sourceJournal, PoolDestinationData sourcePoolDestinationData, Journal targetJournal) {
        try {
            long start = System.currentTimeMillis();

            MessageLocation[] queuesMessageLocations = sourcePoolDestinationData.getQueuesMessageLocations();
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
    //-------------------------------------------------------------------------
    private void renameDirs(File sourceDir, File targetDir) {
        System.out.println("RENAME JOURNALS DIRECTORIES");

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
    //-------------------------------------------------------------------------
    public void optimaze(String sourceDirPath, boolean useAnyKeyToContinue) throws IOException {
        if(isNullOrEmpty(sourceDirPath)) {
            throw new NullPointerException("sourceDirPath");
        }

        File sourceDir = new File(sourceDirPath);
        if(!sourceDir.exists() || !sourceDir.isDirectory()) {
            throw new IllegalArgumentException("sourceDirPath");
        }

        try {
            File targetDir = new File(sourceDirPath + "_temp");
            optimaze(sourceDir, targetDir, useAnyKeyToContinue);

            showSeparator(2);
            renameDirs(sourceDir, targetDir);
            showSeparator(2);
        }
        catch (Throwable throwable) {
            showException(throwable);
        }
    }
    //-------------------------------------------------------------------------
}
