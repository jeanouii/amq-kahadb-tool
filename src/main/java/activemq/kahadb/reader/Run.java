package activemq.kahadb.reader;

public class Run {
    public static void main(String[] args) throws Exception {
        if (args.length <= 0) {
            System.out.println("usage KahaDBJournalsReader <journals directory>");
            System.exit(1);
        }
        //---------------------------------------------------------------------
        String sourceDirPath = args[0];
        boolean useAnyKeyToContinue = false;
        boolean showFileMapCommand = false;
        //---------------------------------------------------------------------
        KahaDBJournalsReader reader = new KahaDBJournalsReader(showFileMapCommand);
        reader.showData(sourceDirPath, useAnyKeyToContinue);
        //---------------------------------------------------------------------
    }
}
