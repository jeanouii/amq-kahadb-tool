package activemq.kahadb.statistic;

public class Run {
    public static void main(String[] args) throws Exception {
        if (args.length <= 0) {
            System.out.println("usage KahaDBJournalsStatistic <journals directory>");
            System.exit(1);
        }
        //---------------------------------------------------------------------
        String sourceDirPath = args[0];
        boolean useAnyKeyToContinue = false;
        //---------------------------------------------------------------------
        KahaDBJournalsStatistics statistics = new KahaDBJournalsStatistics();
        statistics.showStatistic(sourceDirPath, useAnyKeyToContinue);
        //---------------------------------------------------------------------
    }
}
