package org.hill30.activemq.kahadb.utils;

import org.apache.activemq.store.kahadb.data.KahaDestination;
import org.apache.activemq.store.kahadb.disk.journal.Journal;
import org.apache.activemq.store.kahadb.disk.journal.Location;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

public final class KahaDBUtils {
    //-------------------------------------------------------------------------
    public static Journal createJournal(File directory) {
        int journalSize = getJournalSize(directory);
        return createJournal(directory, journalSize);
    }
    public static Journal createJournal(File directory, int journalSize) {
        Journal result = new Journal();

        result.setDirectory(directory);
        result.setMaxFileLength(journalSize);
        result.setCheckForCorruptionOnStartup(false);
        result.setChecksum(false);
        result.setWriteBatchSize(Journal.DEFAULT_MAX_WRITE_BATCH_SIZE);
        result.setArchiveDataLogs(false);

        return result;
    }

    public static int getJournalSize(File directory) {
        Journal journal = new Journal();
        journal.setDirectory(directory);

        int journalSize = Journal.DEFAULT_MAX_FILE_LENGTH;
        try {
            journal.start();
            Location location = journal.getNextLocation(null);
            if(location != null) {
                journalSize = (int)journal.getFile(location.getDataFileId()).length();
            }
        }
        catch (Throwable throwable) {
        }
        finally {
            try {
                journal.close();
            } catch (IOException e) {  }
        }

        return journalSize;
    }
    //-------------------------------------------------------------------------
    public static String bytesToString(long sizeInBytes) {
        final double SPACE_KB = 1024;
        final double SPACE_MB = 1024 * SPACE_KB;
        final double SPACE_GB = 1024 * SPACE_MB;
        final double SPACE_TB = 1024 * SPACE_GB;

        NumberFormat nf = new DecimalFormat();
        nf.setMaximumFractionDigits(2);
        try {
            if (sizeInBytes < SPACE_KB) {
                return nf.format(sizeInBytes) + " Byte(s) (" + sizeInBytes + ")";
            } else if (sizeInBytes < SPACE_MB) {
                return nf.format(sizeInBytes / SPACE_KB) + " KB (" + sizeInBytes + ")";
            } else if (sizeInBytes < SPACE_GB) {
                return nf.format(sizeInBytes / SPACE_MB) + " MB (" + sizeInBytes + ")";
            } else if (sizeInBytes < SPACE_TB) {
                return nf.format(sizeInBytes / SPACE_GB) + " GB (" + sizeInBytes + ")";
            } else {
                return nf.format(sizeInBytes / SPACE_TB) + " TB (" + sizeInBytes + ")";
            }
        } catch (Exception e) {
            return sizeInBytes + " Byte(s)";
        }
    }
    //-------------------------------------------------------------------------
    public static String getDestinationInfo(KahaDestination destination) {
        return getDestinationInfo(destination.getType(), getDestinationId(destination));
    }
    public static String getDestinationInfo(KahaDestination.DestinationType destinationType, String destinationId) {
        return destinationType + " (DestId: " + destinationId + ")";
    }
    //-------------------------------------------------------------------------
    public static String getDestinationId(KahaDestination destination) {
        return destination.getType().getNumber() + ":" + destination.getName();
    }
    //-------------------------------------------------------------------------
    public static boolean isDestinationTopic(KahaDestination destination) {
        return destination.getType() == KahaDestination.DestinationType.TOPIC;
    }
    //-------------------------------------------------------------------------
}
