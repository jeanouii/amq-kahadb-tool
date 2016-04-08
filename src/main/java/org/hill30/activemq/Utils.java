package org.hill30.activemq;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;

public final class Utils {
    //region private
    private static final String Separator = "-----------------------------------------------------------";
    //endregion
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
    public static boolean isNullOrEmpty(String str) {
        return str == null || str.isEmpty();
    }
    //-------------------------------------------------------------------------
    public static void showException(Throwable throwable) {
        System.out.println("Caught: " + throwable);
        throwable.printStackTrace();
        System.exit(1);
    }
    //-------------------------------------------------------------------------
    public static void showSeparator() {
        showSeparator(true);
    }
    public static void showSeparator(int count) {
        if(count < 0) {
            throw new IndexOutOfBoundsException("count");
        }

        for(int i = 0; i < count; ++i) {
            showSeparator(true);
        }
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
    public static void thread(Runnable runnable, boolean daemon) {
        Thread brokerThread = new Thread(runnable);
        brokerThread.setDaemon(daemon);
        brokerThread.start();
    }
    //-------------------------------------------------------------------------
    public static int randInt(Random random, int min, int max) {
        if(random == null) {
            throw new NullPointerException("random");
        }

        return random.nextInt((max - min) + 1) + min;
    }
    //-------------------------------------------------------------------------
}
