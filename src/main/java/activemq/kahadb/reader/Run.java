package activemq.kahadb.reader;

import static activemq.kahadb.utils.KahaDBUtils.pressAnyKeyToExit;

public class Run {
    public static void main(String[] args) throws Exception {
        if (args.length <= 0) {
            System.out.println("usage KahaDBJournalsReader <journals directory>");
            System.exit(1);
        }
        //---------------------------------------------------------------------
        KahaDBJournalsReader reader = new KahaDBJournalsReader(args[0]);
        reader.showData(false, false);
        //---------------------------------------------------------------------
        //pressAnyKeyToExit();
        //---------------------------------------------------------------------
    }
}
