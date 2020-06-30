package storm.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class SummariserBolt extends BaseRichBolt {
    private OutputCollector collector;
    private long tuplesCount;
    private long tuplesSum;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        tuplesCount = 0;
        tuplesSum = 0;
    }

    @Override
    public void execute(Tuple tuple) {
        if (tuple.getValue(0) != null) {
            long time = Long.parseLong((String) tuple.getValue(0));
            tuplesCount+=1;
            tuplesSum+=time;
            long average = tuplesSum/tuplesCount;
            collector.emit(new Values(String.valueOf(tuplesCount), String.valueOf(average)));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("key", "message"));
    }
}
