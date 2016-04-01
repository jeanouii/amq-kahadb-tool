package activemq.tests;

import activemq.Publisher;
import mqtt.MobileClient;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class MainTest {
    //region private
    //=========================================================================
    private static final int SubscribeCount = 1002; //если -1 - бесконечно.
    private static final int CreateSubscribeCount = 2;
    private static final int StartSubscriberId = 1000;
    private static final int ActiveClientCount = 1;

    private static final int CreateSubscribeMinuteTimeDelay = 1;
    private static final int CreateSubscribeSecondTimeDelay = 1;
    //=========================================================================
    private static final long MessageCount = -1; //если -1 - бесконечно.
    private static final long CreateMessageCount = 400;

    private static final int CreateMessageMinuteTimeDelay = 1;
    private static final int CreateMessageSecondTimeDelay = 1;
    //=========================================================================
    private static final String ClientNameTemplate = "Client";
    private static final String TopicNameTemplate="Topic";
    private static final String PublisherNameTemplate = "Publisher";
    //-------------------------------------------------------------------------
    private static final Random rand = new Random();
    private static ArrayList<Integer> createdTopics = new ArrayList<>();
    private static ArrayList<MobileClient> activeClients = new ArrayList<>();
    private static HashMap<String, Integer> clientReceiveMessageCount = new HashMap<>();
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

                    MobileClient client = new MobileClient(ClientNameTemplate + createdSubscribeCount, TopicNameTemplate + createdSubscribeCount);
                    try {
                        client.connect();
                        Thread.sleep(10000);
                        client.disconnect();

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    finally {
                        createdTopics.add(createdSubscribeCount);
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
            while (createdTopics.size() != CreateSubscribeCount) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            String message = "Test message";
            //-----------------------------------------------------------------
            try {
                message = getMessageByFile();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            //-----------------------------------------------------------------
            Publisher publisher = new Publisher(PublisherNameTemplate + 1);
            //-----------------------------------------------------------------
            try {
                publisher.connect();
            } catch (InvalidObjectException e) {
                e.printStackTrace();
                System.exit(1);
            }

            //-----------------------------------------------------------------
            int activeTopicIndex = 0;
            while (true) {
                boolean exit = MessageCount == 0 || CreateMessageCount <= 0;
                long createMessageCount = exit ? 0 : CreateMessageCount;
                for (long i = 0; i < createMessageCount; ++i) {
                    ++createdMessageCount;

                    try {
                        /*if(activeTopicIndex == 0) {
                            publisher.sendMessageInTopic(message, TopicNameTemplate + createdTopics.get(0));
                            ++activeTopicIndex;
                        }
                        else if(activeTopicIndex < 3) {
                            publisher.sendMessageInTopic(message, TopicNameTemplate + createdTopics.get(1));
                            ++activeTopicIndex;
                        }
                        else if(activeTopicIndex < 6) {
                            publisher.sendMessageInTopic(message, TopicNameTemplate + createdTopics.get(2));
                            ++activeTopicIndex;
                        }
                        else {
                            if(activeClients.size() != 0) {
                                Thread.sleep(1000);
                                activeClients.get(0).disconnect();
                                activeClients.clear();
                            }
                            publisher.sendMessageInTopic(message, TopicNameTemplate + createdTopics.get(3));
                        }*/
                        publisher.sendMessageInTopic(message, TopicNameTemplate + createdTopics.get(1));
                    } catch (InvalidObjectException e) {
                        e.printStackTrace();
                    }

                    if(MessageCount > 0 && createdMessageCount == MessageCount) {
                        exit = true;
                        break;
                    }
                }

                exit = true;
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
    //-------------------------------------------------------------------------
    private static void thread(Runnable runnable, boolean daemon) {
        Thread brokerThread = new Thread(runnable);
        brokerThread.setDaemon(daemon);
        brokerThread.start();
    }
    //-------------------------------------------------------------------------
    //endregion
    public static void main(String[] args) throws FileNotFoundException {
        thread(new CreateSubscribeRunnable(), false);
        //thread(new MessageRunnable(), false);
    }
}
