package storm.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class FileWriterBolt extends BaseRichBolt {

    private OutputCollector collector;
    private FileWriter fw;
    private String filePath;

    public FileWriterBolt(String filepath) {
        this.filePath = filepath;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        try {
            fw = new FileWriter(filePath, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        tuple.getValues().forEach(value -> {
            try {
                fw.append(value.toString());
                collector.ack(tuple);
            } catch (IOException e) {
                collector.fail(tuple);
            }
        });
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
