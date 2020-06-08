package storm.util;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;

public class KafkaTuple {

    private String topic;
    private int partition;
    private int offset;
    private String key;
    private String value;

    public KafkaTuple(Tuple tuple) {

        this.topic = tuple.getValueByField("topic").toString();
        this.partition = Integer.parseInt(tuple.getValueByField("partition").toString());
        this.offset = Integer.parseInt(tuple.getValueByField("offset").toString());
        this.key = tuple.getValueByField("key").toString();
        this.value = tuple.getValueByField("value").toString();
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "KafkaTuple{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", offset=" + offset +
                ", key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
