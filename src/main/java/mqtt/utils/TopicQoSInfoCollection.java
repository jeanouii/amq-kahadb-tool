package mqtt.utils;

import java.util.ArrayList;

public class TopicQoSInfoCollection {
    //region private
    private final  ArrayList<String> topicNames = new ArrayList<>();
    private final ArrayList<Integer> qos = new ArrayList<>();
    //-------------------------------------------------------------------------
    private TopicQoSInfo[] cache;
    private boolean recreateCache = true;
    //-------------------------------------------------------------------------
    private void intAdd(String topicName, int qos) {
        if(!topicNames.contains(topicName)) {

            topicNames.add(topicName);
            this.qos.add(qos);

            recreateCache = true;
        }
    }
    private boolean intRemove(String topicName) {
        int index = topicNames.indexOf(topicName);
        if(index != -1) {
            topicNames.remove(index);
            qos.remove(index);
            return true;
        }
        return false;
    }
    //endregion

    public int size() {
        return topicNames.size();
    }
    //-------------------------------------------------------------------------
    public TopicQoSInfo get(int index) {
        if(index < 0 || index >= size()) {
            throw new IndexOutOfBoundsException("index");
        }

        return new TopicQoSInfo(topicNames.get(index), qos.get(index));
    }
    public TopicQoSInfo get(String topicName) {
        TopicQoSInfo topicQoSInfo = null;

        int index = topicNames.indexOf(topicName);
        if(index != -1) {
            topicQoSInfo = new TopicQoSInfo(topicName, qos.get(index));
        }

        return topicQoSInfo;
    }
    //-------------------------------------------------------------------------
    public TopicQoSInfo[] get() {
        if(recreateCache) {
            cache = new TopicQoSInfo[topicNames.size()];

            for (int i = 0; i < topicNames.size(); ++i) {
                cache[i] = new TopicQoSInfo(topicNames.get(i), qos.get(i));
            }
        }
        return cache;
    }
    public TopicQoSInfo[] get(String[] topicNames) {
        if(topicNames == null) {
            throw new NullPointerException("topicNames");
        }

        ArrayList<TopicQoSInfo> topicQoSInfos = new ArrayList<>();

        for (int i = 0; i < topicNames.length; ++i) {
            TopicQoSInfo topicQoSInfo = get(topicNames[i]);
            if(topicQoSInfo != null) {
                topicQoSInfos.add(topicQoSInfo);
            }
        }

        TopicQoSInfo[] result = new TopicQoSInfo[topicQoSInfos.size()];
        return topicQoSInfos.toArray(result);
    }
    //-------------------------------------------------------------------------
    public String[] getTopicNames() {
        String[] result = new String[topicNames.size()];
        return topicNames.toArray(result);
    }
    //-------------------------------------------------------------------------
    public void add(String topicName, int qos) {
        if(topicName == null || topicName.length() == 0) {
            throw new NullPointerException("topicName");
        }

        intAdd(topicName, qos);
    }
    public void add(String[] topicNames, int qos) {
        if(topicNames == null) {
            throw new NullPointerException("topicNames");
        }

        for (int i = 0; i < topicNames.length; ++i) {
            String topicName = topicNames[i];
            if(topicName != null && topicName.length() != 0) {
                intAdd(topicName, qos);
            }
        }
    }
    //-------------------------------------------------------------------------
    public boolean remove(String topicName) {
        if(topicName == null || topicName.length() == 0) {
            throw new NullPointerException("topicName");
        }

        return intRemove(topicName);
    }
    public String[] remove(String[] topicNames) {
        ArrayList<String> removedTopicNames = new ArrayList<>();

        for (int i = 0; i < topicNames.length; ++i) {
            String topicName = topicNames[i];
            if(topicName != null && topicName.length() != 0) {
                if(intRemove(topicName)) {
                    removedTopicNames.add(topicName);
                }
            }
        }

        String[] result = new String[removedTopicNames.size()];
        return removedTopicNames.toArray(result);
    }
    //-------------------------------------------------------------------------
}
