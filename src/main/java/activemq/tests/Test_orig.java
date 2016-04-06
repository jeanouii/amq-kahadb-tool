package activemq.tests;


import activemq.clients.MqttConsumer;
import activemq.clients.Producer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class Test_orig {
    //region private
    //=========================================================================
    private static final int SubscribeCount = -1; //если -1 - бесконечно.
    private static final int CreateSubscribeCount = 1;
    private static final int ActiveClientCount = 900;
    private static final int StartSubscriberId = 1000;

    private static final int CreateSubscribeMinuteTimeDelay = 1;
    private static final int CreateSubscribeSecondTimeDelay = 15;
    //=========================================================================
    private static final long MessageCount = -1; //если -1 - бесконечно.
    private static final long CreateMessageCount = 400;

    private static final int CreateMessageMinuteTimeDelay = 1;
    private static final int CreateMessageSecondTimeDelay = 1;
    //=========================================================================
    private static final int DelayActiveSubscriberMinuteTimeDelay = 2;
    private static final int DelayActiveSubscribeSecondTimeDelay = 60;
    //=========================================================================
    private static final int ActiveSubscribeCount = 200;
    private static final int ActiveSubscriberIterationCount = -1; //если -1 - бесконечно.

    private static final int ActiveSubscriberMinuteTimeDelay = 1;
    private static final int ActiveSubscriberSecondTimeDelay = 5;
    //=========================================================================
    private static final String ClientNameTemplate = "Client1";
    private static final String TopicNameTemplate="Topic";
    private static final String PublisherNameTemplate = "Publisher";
    //-------------------------------------------------------------------------
    private static final Random rand = new Random();
    private static ArrayList<Integer> createdTopics = new ArrayList<>();
    private static ArrayList<MqttConsumer> activeClients = new ArrayList<>();
    //-------------------------------------------------------------------------
    private static class CreateSubscribeRunnable implements Runnable {
        //region private
        private static final long CreateSubscribeTimerDelay = CreateSubscribeMinuteTimeDelay * CreateSubscribeSecondTimeDelay * 1000;
        //---------------------------------------------------------------------
        private int createdSubscribeCount = StartSubscriberId;
        //endregion
        @Override
        public void run() {
            while (true) {
                boolean exit = SubscribeCount == 0 || CreateSubscribeCount <= 0;
                int createSubscribeCount = exit ? 0 : CreateSubscribeCount;
                for (int i = 0; i < createSubscribeCount; ++i) {
                    ++createdSubscribeCount;

                    MqttConsumer client = new MqttConsumer();
                    try {
                        client.connect(ClientNameTemplate + createdSubscribeCount);
                        client.subscribe(TopicNameTemplate + createdSubscribeCount, 2);
                        Thread.sleep(500);
                        client.disconnect();

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    finally {
                        createdTopics.add(createdSubscribeCount);

                        if(activeClients.size() != ActiveClientCount) {
                            activeClients.add(client);
                        }
                    }

                    if(SubscribeCount > 0 && createdSubscribeCount == SubscribeCount) {
                        exit = true;
                        break;
                    }
                }

                if(!exit) {
                    try {
                        Thread.sleep(CreateSubscribeTimerDelay);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                else {
                    break;
                }
            }
        }
    }
    private static class MessageRunnable implements Runnable {
        //region private
        private static final long CreateMessageTimerDelay = CreateMessageMinuteTimeDelay * CreateMessageSecondTimeDelay * 1000;
        //---------------------------------------------------------------------
        private long createdMessageCount = 0;
        //---------------------------------------------------------------------
        public static String getMessageByFile() throws FileNotFoundException {
            return getMessageByFile(Paths.get("./Messages/Message.txt"));
        }
        public static String getMessageByFile(Path pathToFile) throws FileNotFoundException {
            if(pathToFile == null) {
                throw new NullPointerException("pathToFile");
            }

            String absolutePathToFile = pathToFile.toAbsolutePath().toString();
            File file = new File(absolutePathToFile);
            if(!file.isFile()) {
                throw new FileNotFoundException(absolutePathToFile);
            }

            byte[] fileArray = null;
            try {
                fileArray = Files.readAllBytes(pathToFile);
            }
            catch (IOException exception) {
                exception.printStackTrace();
            }

            return fileArray == null ? "" : new String(fileArray);
        }
        //endregion
        @Override
        public void run() {
            /*while (createdTopics.size() != SubscribeCount) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }*/

            String message = "Test message";
            //-----------------------------------------------------------------
            try {
                message = getMessageByFile();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            //-----------------------------------------------------------------
            Producer publisher = new Producer();
            //-----------------------------------------------------------------
            try {
                publisher.connect(PublisherNameTemplate + 1);
            } catch (InvalidObjectException e) {
                e.printStackTrace();
                System.exit(1);
            }
            //-----------------------------------------------------------------
            while (true) {
                boolean exit = MessageCount == 0 || CreateMessageCount <= 0;
                long createMessageCount = exit ? 0 : CreateMessageCount;
                for (long i = 0; i < createMessageCount; ++i) {
                    /*if(createdTopics.size() == 0) {
                        break;
                    }*/

                    ++createdMessageCount;

                    try {
                        //int createdTopicIndex = randInt(0, createdTopics.size() - 1);
                        //int topicIndex = createdTopics.get(createdTopicIndex);
                        int topicIndex = randInt(1, StartSubscriberId);
                        publisher.sendMessageInTopic(message, TopicNameTemplate + topicIndex, false);
                    } catch (InvalidObjectException e) {
                        e.printStackTrace();
                    }

                    if(MessageCount > 0 && createdMessageCount == MessageCount) {
                        exit = true;
                        break;
                    }
                }

                if(!exit) {
                    try {
                        Thread.sleep(CreateMessageTimerDelay);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                else {
                    break;
                }
            }
        }
    }
    private static class ActiveSubscribersRunnable implements Runnable {
        //region private
        private static final int DelayActiveSubscribeTimeDelay = DelayActiveSubscriberMinuteTimeDelay * DelayActiveSubscribeSecondTimeDelay * 1000;
        private static final int ActiveSubscriberTimerDelay = ActiveSubscriberMinuteTimeDelay * ActiveSubscriberSecondTimeDelay * 1000;
        //---------------------------------------------------------------------
        private int activeSubscriberIterationCount = 0;
        //endregion
        @Override
        public void run() {
            while (true) {

                /*while (activeClients.size() != ActiveClientCount) {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }*/

                while (activeSubscriberIterationCount != ActiveSubscriberIterationCount) {
                    try {
                        Thread.sleep(DelayActiveSubscribeTimeDelay);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    ++activeSubscriberIterationCount;
                    int activeClientNumber = ActiveSubscribeCount;//randInt(1, activeClients.size());
                    Set<MqttConsumer> clients = new HashSet<>();

                    for (int i = 0; i < activeClientNumber; ++i) {
                       // int activeClientIndex = randInt(0, activeClients.size() - 1);
                        //MobileClient client = activeClients.get(activeClientIndex);
                        int id = randInt(1, StartSubscriberId);
                        MqttConsumer client = new MqttConsumer();
                        if(client.isConnected() || !clients.add(client))
                        { --i; }
                        else {
                            try {
                                client.connect(ClientNameTemplate + id);
                                client.subscribe(TopicNameTemplate + id, 2);
                            } catch (InvalidObjectException e) {
                                e.printStackTrace();
                                System.exit(1);
                            }
                        }
                    }

                    try {
                        Thread.sleep(ActiveSubscriberTimerDelay);

                        for (MqttConsumer client: clients) {
                            client.disconnect();
                        }
                        clients.clear();

                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                break;
            }
        }
    }
    //-------------------------------------------------------------------------
    private static void thread(Runnable runnable, boolean daemon) {
        Thread brokerThread = new Thread(runnable);
        brokerThread.setDaemon(daemon);
        brokerThread.start();
    }
    //-------------------------------------------------------------------------
    private static int randInt(int min, int max) {
        return rand.nextInt((max - min) + 1) + min;
    }
    //-------------------------------------------------------------------------
    //endregion
    public static void main(String[] args) throws FileNotFoundException {
        thread(new CreateSubscribeRunnable(), false);
        thread(new MessageRunnable(), false);
        thread(new ActiveSubscribersRunnable() , false);
    }
}

