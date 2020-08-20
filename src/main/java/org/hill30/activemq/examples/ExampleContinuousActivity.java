package org.hill30.activemq.examples;

import org.hill30.activemq.clients.MqttConsumer;
import org.hill30.activemq.clients.Producer;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Random;

import static org.hill30.activemq.Utils.randInt;
import static org.hill30.activemq.Utils.showException;
import static org.hill30.activemq.Utils.thread;

public class ExampleContinuousActivity {
    //region private
    private static final int SubscribeCount = -1; //if equal '-1' - infinity.
    private static final int FirstSubscribeId = 1000;
    private static final int MinuteDelayCreatingNextSubscribe = 1;
    private static final int SecondDelayCreatingNextSubscribe = 0;
    //-------------------------------------------------------------------------
    private static final int CreatingMessageIterationCount = -1; //if equal '-1' - infinity.
    private static final int CreatingMessageCount = 400;
    private static final int MinuteDelayCreatingNextMessagesIteration = 0;
    private static final int SecondDelayCreatingNextMessagesIteration = 10;
    private static final String PathToFileMessage = "../../Messages/Message.txt";
    //-------------------------------------------------------------------------
    private static final int ActivingConsumerIterationCount = -1; //if equal '-1' - infinity.
    private static final int ActivingConsumerCount = 200;
    private static final int MinuteDelayNextActivingConsumersIteration = 0;
    private static final int SecondDelayNextActivingConsumersIteration = 20;
    private static final int MinuteDelayActivedConsumers = 0;
    private static final int SecondDelayActivedConsumers = 15;
    //-------------------------------------------------------------------------
    private static final String ClientIdFixedName = "test.client";
    private static final String TopicFixedName ="test/topic";
    private static final String ProducerClientIdFixedName = "test.producer";
    //-------------------------------------------------------------------------
    private static class CreatingSubscribes implements Runnable {
        //region private
        private static final long DelayCreatingNextSubscribe = MinuteDelayCreatingNextSubscribe * 60 * 1000
                + (SecondDelayCreatingNextSubscribe < 0 ? 0 : SecondDelayCreatingNextSubscribe * 1000);
        //endregion
        //---------------------------------------------------------------------
        public static int NextSubscribeId = FirstSubscribeId;
        //---------------------------------------------------------------------
        @Override
        public void run() {
            if(SubscribeCount < -1 || SubscribeCount == 0) {
                return;
            }
            if(FirstSubscribeId < 0) {
                return;
            }
            //-----------------------------------------------------------------
            NextSubscribeId = FirstSubscribeId;
            while (SubscribeCount < 0 || ((NextSubscribeId - FirstSubscribeId) != SubscribeCount)) {
                String clientId = String.format("%s.%s", ClientIdFixedName, NextSubscribeId);
                String topicName = String.format("%s/%s", TopicFixedName, NextSubscribeId);

                MqttConsumer consumer = new MqttConsumer(new MqttConsumer.IMessageAdapter() {
                    @Override
                    public void showMessage(String message) {
                        System.out.println(message);
                    }
                    @Override
                    public void showMessage(MqttConsumer.MessageInfo message) {
                        message.show();
                    }
                });

                try {
                    consumer.connect(clientId);
                    consumer.addTopicAfterConnect(topicName);
                    Thread.sleep(1000);
                }
                catch (Throwable throwable) {
                    showException(throwable);
                }
                finally {
                    consumer.disconnect();
                }

                if(NextSubscribeId != FirstSubscribeId) {
                    try {
                        Thread.sleep(DelayCreatingNextSubscribe);
                    } catch (Throwable throwable) {
                        showException(throwable);
                    }
                }

                ++NextSubscribeId;
                if(NextSubscribeId == Integer.MAX_VALUE) {
                    break;
                }
            }
        }
        //---------------------------------------------------------------------
    }
    private static class MessageRunnable implements Runnable {
        //region private
        private static final long DelayCreatingNextMessagesIteration = MinuteDelayCreatingNextMessagesIteration * 60 * 1000
                + (SecondDelayCreatingNextMessagesIteration < 0 ? 0 : SecondDelayCreatingNextMessagesIteration * 1000);
        //---------------------------------------------------------------------
        private static String getMessageByFile(Path pathToFile) {
            if(pathToFile == null) {
                throw new NullPointerException("pathToFile");
            }

            String absolutePathToFile = pathToFile.toAbsolutePath().toString();
            File file = new File(absolutePathToFile);
            Boolean isFile = file.isFile();
            if(isFile) {
                byte[] fileArray = null;
                try {
                    fileArray = Files.readAllBytes(pathToFile);
                }
                catch (Throwable throwable) {
                    showException(throwable);
                }

                if(fileArray != null) {
                    return new String(fileArray);
                }
            }

            return "Enter some text here for the message body...";
        }
        //endregion
        @Override
        public void run() {
            if(CreatingMessageIterationCount < -1 || CreatingMessageIterationCount == 0) {
                return;
            }
            if(FirstSubscribeId < 0) {
                return;
            }
            //-----------------------------------------------------------------
            while (CreatingSubscribes.NextSubscribeId == FirstSubscribeId) {
                try {
                    Thread.sleep(500);
                }
                catch (Throwable throwable) {
                    showException(throwable);
                }
            }
            //-----------------------------------------------------------------
            Producer producer = new Producer(message -> System.out.println(message));
            String clientId = String.format("%s.%s", ProducerClientIdFixedName, 1);

            try {
                producer.connect(clientId);

                if(producer.isConnected()) {
                    String message = getMessageByFile(Paths.get(PathToFileMessage));
                    int maxCreatedMessageCount = CreatingMessageCount < 1 ? 1 : CreatingMessageCount;
                    Random topicIdRandom = new Random();

                    long nextIterationCounter = 0;
                    while (nextIterationCounter != CreatingMessageIterationCount) {
                        for(int i = 0; i < maxCreatedMessageCount; ++i) {
                            int topicId = randInt(topicIdRandom, FirstSubscribeId, CreatingSubscribes.NextSubscribeId);
                            String topicName = String.format("%s.%s", TopicFixedName.replace("/","."), topicId);
                            producer.sendMessageInTopic(message, topicName);
                        }

                        try {
                            Thread.sleep(DelayCreatingNextMessagesIteration);
                        }
                        catch (Throwable throwable) {
                            showException(throwable);
                        }

                        if(nextIterationCounter != Integer.MAX_VALUE) {
                            ++nextIterationCounter;
                        }
                    }
                }
            }
            catch (Throwable throwable) {
                showException(throwable);
            }
            finally {
                producer.disconnect();
            }
        }
    }
    private static class ActivingConsumers implements Runnable {
        //region private
        private static final long DelayNextActivingConsumersIteration = MinuteDelayNextActivingConsumersIteration * 60 * 1000
                + (SecondDelayNextActivingConsumersIteration < 0 ? 0 : SecondDelayNextActivingConsumersIteration * 1000);
        private static final long DelayActivedConsumers = MinuteDelayActivedConsumers * 60 * 1000
                + (SecondDelayActivedConsumers < 0 ? 0 : SecondDelayActivedConsumers * 1000);
        //endregion
        @Override
        public void run() {
            if(ActivingConsumerIterationCount < -1 || ActivingConsumerIterationCount == 0) {
                return;
            }
            if(FirstSubscribeId < 0) {
                return;
            }
            //-----------------------------------------------------------------
            while (CreatingSubscribes.NextSubscribeId == FirstSubscribeId) {
                try {
                    Thread.sleep(500);
                }
                catch (Throwable throwable) {
                    showException(throwable);
                }
            }
            //-----------------------------------------------------------------
            Random consumerIdRandom = new Random();
            int maxActivingConsumerCount = ActivingConsumerCount < 1 ? 1 : ActivingConsumerCount;
            HashMap<Integer, MqttConsumer> activingConsumers = new HashMap<>(maxActivingConsumerCount);

            int nextActivingConsumers = 0;
            while (nextActivingConsumers != ActivingConsumerIterationCount) {
                int nextSubscriptionId = CreatingSubscribes.NextSubscribeId;
                int subscriptionCount = nextSubscriptionId - FirstSubscribeId;
                int realActivingConsumerCount = subscriptionCount < maxActivingConsumerCount ? subscriptionCount : maxActivingConsumerCount;

                for (int i = 0; i < realActivingConsumerCount; ++i) {
                    int consumerId = randInt(consumerIdRandom, FirstSubscribeId, nextSubscriptionId);

                    if (!activingConsumers.containsKey(consumerId)) {
                        String clientId = String.format("%s.%s", ClientIdFixedName, consumerId);
                        String topicName = String.format("%s/%s", TopicFixedName, i);

                        MqttConsumer consumer = new MqttConsumer(new MqttConsumer.IMessageAdapter() {
                            @Override
                            public void showMessage(String message) {
                                System.out.println(message);
                            }
                            @Override
                            public void showMessage(MqttConsumer.MessageInfo message) {
                                message.show();
                            }
                        });

                        try {
                            consumer.connect(clientId);
                            consumer.addTopicAfterConnect(topicName);
                        }
                        catch (Throwable throwable) {}

                        activingConsumers.put(consumerId, consumer);
                    }
                }

                if(activingConsumers.size() != 0) {
                    try {
                        Thread.sleep(DelayActivedConsumers);
                    } catch (Throwable throwable) {
                        showException(throwable);
                    }

                    activingConsumers.values().forEach(MqttConsumer::disconnect);
                }

                try {
                    Thread.sleep(DelayNextActivingConsumersIteration);
                } catch (Throwable throwable) {
                    showException(throwable);
                }

                if (nextActivingConsumers != Integer.MAX_VALUE) {
                    ++nextActivingConsumers;
                }
            }
        }
    }
    //endregion
    public static void main(String[] args) {
        thread(new CreatingSubscribes(), false);
        thread(new MessageRunnable(), false);
        thread(new ActivingConsumers() , false);
    }
}

