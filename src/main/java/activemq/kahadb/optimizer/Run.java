package activemq.kahadb.optimizer;

import static activemq.kahadb.utils.KahaDBUtils.pressAnyKeyToExit;

public class Run {
    public static void main(String[] args) throws Exception {
        if(args.length <= 0) {
            System.out.println("usage KahaDBJournalsOptimizer <journals directory>");
            System.exit(1);
        }
        //---------------------------------------------------------------------
        KahaDBJournalsOptimizer kahaDBJournalsOptimizer = new KahaDBJournalsOptimizer(args[0]);
        kahaDBJournalsOptimizer.optimaze();
        //---------------------------------------------------------------------
        //pressAnyKeyToExit();
        //---------------------------------------------------------------------
    }
}
