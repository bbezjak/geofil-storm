//package storm.bolts;
//
//import com.vividsolutions.jts.geom.Geometry;
//import com.vividsolutions.jts.index.strtree.STRtree;
//import org.apache.storm.task.OutputCollector;
//import org.apache.storm.task.TopologyContext;
//import org.apache.storm.topology.OutputFieldsDeclarer;
//import org.apache.storm.topology.base.BaseRichBolt;
//import org.apache.storm.tuple.Fields;
//import org.apache.storm.tuple.Tuple;
//import org.apache.storm.tuple.Values;
//import org.geotools.geojson.geom.GeometryJSON;
//import org.locationtech.jts.geom.Geometry;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import storm.tuples.GeoType;
//
//import java.io.IOException;
//import java.nio.file.Files;
//import java.nio.file.Paths;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.stream.Stream;
//
//public class GeoIndexBoltJts extends BaseRichBolt {
//
//    private static final Logger LOG = LoggerFactory.getLogger(GeoIndexBoltJts.class);
//    String subscriptionsFilePath;
//
//    private OutputCollector collector;
//    private int tupleCounter = 0;
//
//    // TODO add dynamical choice of geoIndex
//    STRtree subscriptionIndex = new STRtree();
//
//    // timer variables
//    private long processedPublications = 0;
//    private long processingTime = 0;
//
//    public GeoIndexBoltJts(String filePath) {
//        this.subscriptionsFilePath = filePath;
//    }
//
//    @Override
//    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
//        LOG.info("--> Preparation of " + this.getClass().getName() + " started");
//        this.collector = outputCollector;
//
//        try {
//            GeometryJSON gj = new GeometryJSON(19);
//            Stream<String> lines =
//                    Files.lines(Paths.get(subscriptionsFilePath));
//
//            AtomicInteger counter = new AtomicInteger();
//
//            lines.forEach(line -> {
//                Geometry geometry = null;
//                try {
//                    geometry = gj.read(line);
//                    subscriptionIndex.insert(geometry.getEnvelopeInternal(), geometry);
//                    counter.getAndIncrement();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            });
//        } catch (IOException e) {
//            System.err.println(subscriptionIndex.size());
//            e.printStackTrace();
//        }
//
//        // TODO
//        // https://stackoverflow.com/questions/38221646/understanding-the-storm-architecture
//        // pogledaj obavezno
//
//        LOG.info("--> Initialization of " + this.getClass().getName() + " ended successfully");
//    }
//
//    @Override
//    public void execute(Tuple tuple) {
//
//        if (tuple.getValue(0) != null) {
//            LOG.info("--> " + this.getClass().getName() + " received tuple " + tuple.toString());
//            // expected tuple structure: type:GeoType, geometry:Geometry, decimals:int
//
//            String typeString = ((String) tuple.getValue(0)).toUpperCase();
//            GeoType type = GeoType.valueOf(typeString);
//
//            int decimals = Integer.parseInt(tuple.getValue(2).toString());
//
//            GeometryJSON gj = new GeometryJSON(decimals);
//            Geometry publication = null;
//            try {
//                publication = gj.read(tuple.getValue(1));
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//
//            long startTime = System.currentTimeMillis();
//            List<Geometry> matchingSubscriptions = subscriptionIndex.query(publication.getEnvelopeInternal());
//
//            int subscriptionCounter = 0;
//            for (Geometry matchingSubscription : matchingSubscriptions) {
//
//                if (matchingSubscription.covers(publication)) {
//                    subscriptionCounter++;
//                }
//            }
//
//            long endTime = System.currentTimeMillis() - startTime;
//            processedPublications++;
//            processingTime += endTime;
//
//            tupleCounter++;
//            String pubStr = publication.toString() + "\n";
//            String matchedStr = tupleCounter + ": Publication matched " + subscriptionCounter + " subscriptions\n";
//            String avgProcTime = "Average processing time: " + processingTime / processedPublications + " milis\n";
//
//            collector.emit(new Values(pubStr, matchedStr, avgProcTime));
//        }
//    }
//
//    @Override
//    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declare(new Fields("publication", "matched", "avgProcessingTime"));
//    }
//
//    @Override
//    public void cleanup() {
//        System.out.print("Average processing time:");
//        System.out.println(processedPublications / processingTime);
//    }
//}
