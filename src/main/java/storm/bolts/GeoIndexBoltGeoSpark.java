//package storm.bolts;
//
//import com.vividsolutions.jts.geom.Geometry;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.storm.task.OutputCollector;
//import org.apache.storm.task.TopologyContext;
//import org.apache.storm.topology.OutputFieldsDeclarer;
//import org.apache.storm.topology.base.BaseRichBolt;
//import org.apache.storm.tuple.Fields;
//import org.apache.storm.tuple.Tuple;
//import org.apache.storm.tuple.Values;
//import org.datasyslab.geospark.enums.GridType;
//import org.datasyslab.geospark.spatialOperator.RangeQuery;
//import org.datasyslab.geospark.spatialRDD.SpatialRDD;
//import org.geotools.geojson.geom.GeometryJSON;
//import org.locationtech.jts.geom.Geometry;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import scala.Tuple2;
//import storm.tuples.GeoType;
//
//import java.io.IOException;
//import java.nio.file.Files;
//import java.nio.file.Paths;
//import java.util.*;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.stream.Stream;
//
//public class GeoIndexBoltGeoSpark extends BaseRichBolt {
//
//    private static final Logger LOG = LoggerFactory.getLogger(GeoIndexBoltGeoSpark.class);
//    String subscriptionsFilePath;
//
//    private OutputCollector collector;
//    private int tupleCounter = 0;
//    private SpatialRDD<Geometry> spatialRDD;
//
//    //
//    // TODO initialize collection of subscriptions from file
//    // read all lines from file and save only geometry part, type is not necessary
//    // to create distributed index each node should read only part of subscription set (I guess)
//    // to create replicated index each node should have local copy of whole index (I guess)
//
//
//    private List<Geometry> subscriptions = null;
//
//    // timer variables
//    private long processedPublications = 0;
//    private long processingTime = 0;
//
//    public GeoIndexBoltGeoSpark(String filePath) {
//        this.subscriptionsFilePath = filePath;
//    }
//
//    @Override
//    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
//        LOG.info("--> Preparation of " + this.getClass().getName() + " started");
//        this.collector = outputCollector;
//
//        try {
//
//            GeometryJSON gj = new GeometryJSON(19);
//            Stream<String> lines =
//                    Files.lines(Paths.get(subscriptionsFilePath));
//
//            subscriptions = new LinkedList<>();
//            AtomicInteger counter = new AtomicInteger();
//
//            lines.forEach(line -> {
//                Geometry geometry = null;
//                try {
//                    geometry = gj.read(line);
//                    subscriptions.add(geometry);
//                    counter.getAndIncrement();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            });
//        } catch (IOException e) {
//            System.err.println(subscriptions.size());
//            e.printStackTrace();
//        }
//
//        // TODO
//        // https://stackoverflow.com/questions/38221646/understanding-the-storm-architecture
//        // pogledaj obavezno
//
//        SparkConf conf = new SparkConf().setAppName("spark-library").setMaster("local[*]");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//        GridType gridType = GridType.RTREE;
//        JavaRDD<Geometry> subscriptionsRdd = sc.parallelize(subscriptions);
//
//        spatialRDD = new SpatialRDD<>();
//        try {
//            spatialRDD.setRawSpatialRDD(subscriptionsRdd);
//            spatialRDD.analyze();
//            spatialRDD.spatialPartitioning(gridType);
//            //spatialRDD.buildIndex(IndexType.RTREE, false);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        LOG.info("--> Initialization of " + this.getClass().getName() + " ended successfully");
//    }
//
//    @Override
//    public void execute(Tuple tuple) {
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
//            List<Geometry> matchingSubscriptions = getPartitionsForPublication(publication);
//
//            int subscriptionCounter = 0;
//            for (Geometry matchingSubscription : matchingSubscriptions) {
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
//
//    private List<Geometry> getPartitionsForPublication(Geometry publication) {
//        List<Geometry> subscriptions = new ArrayList<>();
//
//        try {
//            Iterator<Tuple2<Integer, Geometry>> iterator =
//                    spatialRDD.getPartitioner().placeObject(publication);
//            while (iterator.hasNext()) {
//                subscriptions.add(iterator.next()._2);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return subscriptions;
//    }
//
//    private List<Geometry> getPartitionsForPublication2(Geometry publication) {
//        List<Geometry> subscriptions = new ArrayList<>();
//        boolean considerBoundaryIntersection = false;
//        boolean usingIndex = spatialRDD.indexedRawRDD != null;
//
//        try {
//            JavaRDD<Geometry> rdd =
//                    RangeQuery.SpatialRangeQuery(
//                            spatialRDD,
//                            publication.getEnvelopeInternal(),
//                            considerBoundaryIntersection, usingIndex);
//            subscriptions = rdd.collect();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return subscriptions;
//    }
//}
