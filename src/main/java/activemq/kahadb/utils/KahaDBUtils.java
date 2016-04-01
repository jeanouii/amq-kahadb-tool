package activemq.kahadb.utils;

import org.apache.activemq.store.kahadb.data.KahaDestination;
import org.apache.activemq.store.kahadb.disk.journal.Journal;
import org.apache.activemq.store.kahadb.disk.journal.Location;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.text.NumberFormat;

public class KahaDBUtils {
    //region private
    private static final String Separator = "--------------------------------------------------------------------------------";
    //endregion
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
    public static void deleteDir(File file) {
        File[] contents = file.listFiles();
        if (contents != null) {
            for (File f : contents) {
                deleteDir(f);
            }
        }
        file.delete();
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
    public static boolean isNullOrEmpty(String str) {
        return str == null || str.isEmpty();
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
    public static void showException(Throwable throwable) {
        System.out.println("Caught: " + throwable);
        throwable.printStackTrace();
        System.exit(1);
    }
    public static void showSeparator() {
        showSeparator(true);
    }
    public static void showSeparator(boolean newLine) {
        if(newLine) {
            System.out.println(Separator);
        }
        else {
            System.out.printf(Separator);
        }
    }
    //-------------------------------------------------------------------------
    public static void pressAnyKeyToExit() throws IOException {
        System.out.printf("Press any key to end.");
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        br.readLine();
        System.exit(0);
    }
    public static void pressAnyKeyToContinue() throws IOException {
        System.out.printf("Press any key to continues.");
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        br.readLine();
    }
    //-------------------------------------------------------------------------
}
