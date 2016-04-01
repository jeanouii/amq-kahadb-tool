package mqtt.utils;

public class TopicQoSInfo {
    //region private
    private final String topicName;
    private final int qos;
    //endregion
    public TopicQoSInfo(String topicName, int qos) {
        this.topicName = topicName;
        this.qos = qos;
    }

    //-------------------------------------------------------------------------
    public String getTopicName() {
        return topicName;
    }
    public int getQoS() {
        return qos;
    }
    //-------------------------------------------------------------------------
    public String getQoSString() {
        switch (qos) {
            case 0: {
                return "AT_MOST_ONCE";
            }
            case 1: {
                return "AT_LEAST_ONCE";
            }
            case 2: {
                return "EXACTLY_ONCE";
            }
            default: {
                return "-";
            }
        }
    }
    //-------------------------------------------------------------------------
}
