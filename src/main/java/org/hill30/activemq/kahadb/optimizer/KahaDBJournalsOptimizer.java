package org.hill30.activemq.kahadb.optimizer;

import org.apache.activemq.store.kahadb.disk.journal.Journal;
import org.apache.activemq.store.kahadb.disk.journal.Location;
import org.apache.activemq.util.ByteSequence;
import org.hill30.activemq.kahadb.optimizer.destinations.PoolDestinationData;

import java.io.File;
import java.io.IOException;

import static org.hill30.activemq.Utils.deleteDir;
import static org.hill30.activemq.Utils.isNullOrEmpty;
import static org.hill30.activemq.Utils.pressAnyKeyToContinue;
import static org.hill30.activemq.Utils.showException;
import static org.hill30.activemq.Utils.showSeparator;
import static org.hill30.activemq.kahadb.utils.KahaDBUtils.bytesToString;
import static org.hill30.activemq.kahadb.utils.KahaDBUtils.createJournal;
import static org.hill30.activemq.kahadb.utils.KahaDBUtils.getJournalSize;

public final class KahaDBJournalsOptimizer {
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
        System.out.println();
        System.out.printf("- Directory: '%s'.\r\n", sourceJournal.getDirectory().getPath());
        System.out.println();
        System.out.printf("- Journal size: %s.\r\n", bytesToString(sourceJournal.getMaxFileLength()));

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
            showCount("- Topics: %s.", poolDestinationData.getTopicCount());
            showCount("- Queues: %s.", poolDestinationData.getQueueCount());
            showCount("- Committed transactions: %s.", poolDestinationData.getCommittedTransactionCount());
            showCount("- Prepared transactions: %s.", poolDestinationData.getPreparedTransactionCount());
            System.out.println();
            System.out.printf("- It took time: %s seconds.\r\n", ((end - start) / 1000.0f));
        }
        catch (Throwable throwable) {
            showException(throwable);
        }
        finally {
            poolDestinationData.gc();

            try {
                sourceJournal.close();
            } catch (IOException e) {  }
        }

        return poolDestinationData;
    }
    //-------------------------------------------------------------------------
    private void dataOptimization(Journal sourceJournal, PoolDestinationData sourcePoolDestinationData, Journal targetJournal) {
        System.out.println("START JOURNALS DATA OPTIMIZATION");
        System.out.println();

        try {
            long start = System.currentTimeMillis();

            targetJournal.start();

            subscriptionsMove(sourceJournal, sourcePoolDestinationData.getSubscriptionLocations(), targetJournal);
            messagesMove(sourceJournal, sourcePoolDestinationData.getMessageLocations(), targetJournal);

            long end = System.currentTimeMillis();
            System.out.printf("- Journals remained: %s (Total size: %s).\n\r", targetJournal.getFiles().size(), bytesToString(targetJournal.getDiskSize()));
            System.out.println();
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
    private void subscriptionsMove(Journal sourceJournal, Location[] locations, Journal targetJournal) {
        if(locations.length == 0) {
            return;
        }

        try {
            long start = System.currentTimeMillis();

            sourceJournal.start();

            for(Location location : locations) {
                ByteSequence sequence = sourceJournal.read(location);
                targetJournal.write(sequence, true);
            }

            long end = System.currentTimeMillis();
            System.out.printf("- Subscriptions moved: %s (It took time: %s seconds).\r\n", locations.length, ((end - start) / 1000.0f));
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
    private void messagesMove(Journal sourceJournal, Location[] locations, Journal targetJournal) {
        if(locations.length == 0) {
            return;
        }

        try {
            long start = System.currentTimeMillis();

            sourceJournal.start();

            for(Location location : locations) {
                ByteSequence sequence = sourceJournal.read(location);
                targetJournal.write(sequence, true);
            }

            long end = System.currentTimeMillis();
            System.out.printf("- Messages moved: %s (It took time: %s seconds).\r\n", locations.length, ((end - start) / 1000.0f));
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
        System.out.println();

        try {
            String sourceDirPath = sourceDir.getCanonicalPath();
            String newSourceDirPath = sourceDirPath + "_" + (System.currentTimeMillis());

            File newSourceDir = new File(newSourceDirPath);
            newSourceDir.deleteOnExit();

            sourceDir.renameTo(newSourceDir);
            targetDir.renameTo(new File(sourceDirPath));

            System.out.printf("- Directory optimized: '%s'.\r\n", sourceDirPath);
            System.out.printf("- Backup directory: '%s'.\r\n", newSourceDirPath);

            System.out.println();

        } catch (Throwable throwable) {
            showException(throwable);
        }
    }
    //-------------------------------------------------------------------------
    private void showCount(String format, int count) {
        if(count != 0) {
            System.out.printf(format + "\r\n", count);
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
