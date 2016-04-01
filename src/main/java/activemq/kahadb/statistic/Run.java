package activemq.kahadb.statistic;

import static activemq.kahadb.utils.KahaDBUtils.pressAnyKeyToExit;

public class Run {
    public static void main(String[] args) throws Exception {
        if (args.length <= 0) {
            System.out.println("usage KahaDBJournalsStatistic <journals directory>");
            System.exit(1);
        }
        //---------------------------------------------------------------------
        KahaDBJournalsStatistics statistics = new KahaDBJournalsStatistics(args[0]);
        statistics.showStatistic(false);
        //---------------------------------------------------------------------
        //pressAnyKeyToExit();
        //---------------------------------------------------------------------
    }
}
