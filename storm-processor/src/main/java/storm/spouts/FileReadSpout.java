package storm.spouts;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;

public class FileReadSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(FileReadSpout.class);
    SpoutOutputCollector collector;

    String publicationsFilename = "publications19.json";
    private BufferedReader publicationFileReader;
    private ObjectMapper mapper;
    private int decimalNumber;

    private int tupleCounter = 0;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        LOG.info("--> Initialization of " + this.getClass().getName() + " started");
        this.collector = spoutOutputCollector;
        decimalNumber = 19;

        try{
            publicationFileReader = new BufferedReader(new
                    FileReader(publicationsFilename));
            mapper = new ObjectMapper();
        }
        catch(Exception ex){
            LOG.info("--> Initialization of " + this.getClass().getName() + " failed");
            ex.printStackTrace();
        }

        LOG.info("--> Initialization of " + this.getClass().getName() + " ended successfully");
    }

    @Override
    public void nextTuple() {
        //LOG.info("--> " + this.getClass().getName() + " generating tuple " + tupleCounter);

        String publication;
        if(publicationFileReader != null){
            try {
                publication = publicationFileReader.readLine();
                if(publication != null) {
                    Map<String, String> keyValue = this.mapper.readValue(publication, Map.class);
                    collector.emit(new Values(keyValue.get("type"), publication, String.valueOf(decimalNumber)));
                    tupleCounter++;
                    LOG.info("--> " + this.getClass().getName() + " generated tuple " + tupleCounter + " and emitted");
                } else {
                    collector.emit(new Values(null, null, null));
                    //LOG.info("--> " + this.getClass().getName() + " generated tuple and emitted empty tuple");
                }
            }
            catch(Exception ex){
                LOG.info("--> " + this.getClass().getName() + " exception during tuple generation");
                ex.printStackTrace();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("type", "publication", "decimalNumber"));
    }
}
