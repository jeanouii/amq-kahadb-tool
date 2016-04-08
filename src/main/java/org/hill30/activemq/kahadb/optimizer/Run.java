package org.hill30.activemq.kahadb.optimizer;

public final class Run {
    public static void main(String[] args) throws Exception {
        if(args.length <= 0) {
            System.out.println("usage KahaDBJournalsOptimizer <journals directory>");
            System.exit(1);
        }
        //---------------------------------------------------------------------
        String sourceDirPath = args[0];
        boolean useAnyKeyToContinue = false;
        //---------------------------------------------------------------------
        KahaDBJournalsOptimizer kahaDBJournalsOptimizer = new KahaDBJournalsOptimizer();
        kahaDBJournalsOptimizer.optimaze(sourceDirPath, useAnyKeyToContinue);
        //---------------------------------------------------------------------
    }
}
