package activemq.tests;

import activemq.Publisher;
import mqtt.MobileClient;
import org.apache.activemq.store.kahadb.JournalCommand;
import org.apache.activemq.store.kahadb.data.KahaEntryType;
import org.apache.activemq.store.kahadb.data.KahaSubscriptionCommand;
import org.apache.activemq.store.kahadb.disk.journal.Journal;
import org.apache.activemq.store.kahadb.disk.journal.Location;
import org.apache.activemq.store.kahadb.disk.util.DataByteArrayInputStream;
import org.apache.activemq.util.ByteSequence;

import java.io.File;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SubscriptionsMessagesTest {
    //region
    private static final int JornalFileMaxLength = Journal.DEFAULT_MAX_FILE_LENGTH;//32 * 1024 * 1024; //32 МБ
    private static final int BatchSize = Journal.DEFAULT_MAX_WRITE_BATCH_SIZE;
    private final Journal journal;
    private HashMap<String, String> clientAndTopics = new HashMap<>();
    private List<MobileClient> clients = new ArrayList<>();
    long messageCount = 0;
    private final String message="Test Message";
    //-------------------------------------------------------------------------
    private void subscribeCommand(KahaSubscriptionCommand message) {
        if(message.hasSubscriptionInfo()) {
            String subscriptionKey = message.getSubscriptionKey();
            int index = subscriptionKey.indexOf(":");
            if(index > 0) {
                String clientId = subscriptionKey.substring(0, index);
                String topicName = message.getDestination().getName();
                clientAndTopics.put(clientId, topicName);
            }
        }
    }
    //endregion
    public SubscriptionsMessagesTest(String journalDirPathName) {
        if(journalDirPathName == null || journalDirPathName.length() == 0) {
            throw new NullPointerException("journalDirPathName");
        }

        File journalDir = new File(journalDirPathName);
        if(!journalDir.isDirectory()) {
            throw new IllegalArgumentException("journalDirPathName");
        }

        journal = new Journal();
        journal.setDirectory(journalDir);
        journal.setMaxFileLength(JornalFileMaxLength);
        journal.setCheckForCorruptionOnStartup(false);
        journal.setChecksum(false);
        journal.setWriteBatchSize(BatchSize);
        journal.setArchiveDataLogs(false);
    }
    //-------------------------------------------------------------------------
    public static void main(String[] args) throws Exception {
        if(args.length != 1) {
            System.out.println("usage JournalReader <journal data directory>");
            System.exit(1);
        }


        SubscriptionsMessagesTest reader = new SubscriptionsMessagesTest(args[0]);
        //reader.Parse();
        reader.Clients();
        Thread.sleep(10000);
       // reader.Publisher();
        //System.in.read();
        System.out.println("EXIT");
    }
    //-------------------------------------------------------------------------
    public void Parse() throws IOException {
        System.out.println("PARSE");
        journal.start();

        Location location = journal.getNextLocation(null);
        while (location != null) {
            //-----------------------------------------------------------------
            ByteSequence sequence = journal.read(location);
            DataByteArrayInputStream sequenceDataStream = new DataByteArrayInputStream(sequence);
            KahaEntryType commandType = KahaEntryType.valueOf(sequenceDataStream.readByte());

            if(commandType == KahaEntryType.KAHA_SUBSCRIPTION_COMMAND) {
                JournalCommand<?> message = (JournalCommand<?>)commandType.createMessage();
                message.mergeFramed(sequenceDataStream);

                subscribeCommand((KahaSubscriptionCommand)message);
            }

            location = journal.getNextLocation(location);
        }

        journal.close();

        System.out.println("EXIT PARSE");
    }
    public void Clients() throws InvalidObjectException, InterruptedException {
        clientAndTopics.put("client600", "topic600");

        System.out.println("CLIENTS");
        for (Map.Entry<String, String> clientTopic : clientAndTopics.entrySet()) {
            MobileClient client = new MobileClient(clientTopic.getKey());
            //clients.add(client);

            client.connect();
            Thread.sleep(5000);
            client.disconnect();
        }
        System.out.println("EXIT CLIENTS");
    }
    public void Publisher() throws InvalidObjectException, InterruptedException {
        System.out.println("PUBLISHER");
        Publisher publisher = new Publisher("12313424356467888");
        publisher.connect();

        int i = 0;
        for (Map.Entry<String, String> clientTopic : clientAndTopics.entrySet()) {
            /*MobileClient client = clients.get(i);
            if(client.getMessageCount() != 0) {
                for(String message : client.getMessages()) {
                    ++messageCount;
                    publisher.sendMessageInTopic(message, clientTopic.getValue());
                }
                Thread.sleep(50);
            }*/
            ++messageCount;
            publisher.sendMessageInTopic(message, clientTopic.getValue());
            ++i;
        }


        publisher.disconnect();
        System.out.printf("EXIT PUBLISHER (Messages: %s, Topics: %s).\r\n", messageCount, clientAndTopics.size());
    }
    //-------------------------------------------------------------------------
}
