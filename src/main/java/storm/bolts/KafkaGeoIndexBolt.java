package storm.bolts;

import hr.fer.retrofit.geofil.indexing.PartitionedSpatialIndexFactory;
import hr.fer.retrofit.geofil.indexing.SpatialIndexFactory;
import hr.fer.retrofit.geofil.partitioning.SpatialPartitionerFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner;
import org.geotools.geojson.geom.GeometryJSON;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.SpatialIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import storm.util.KafkaTuple;
import storm.util.TopologyConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static storm.util.ThrowingFunction.unchecked;

public class KafkaGeoIndexBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(GeoIndexBolt.class);

    private OutputCollector collector;
    private int tupleCounter;

    // timer variables
    private long processedPublications = 0;
    private long processingTime = 0;

    private int NUMBER_OF_PARTITIONS;
    private int DECIMALS = 19;
    private GridType GRID_TYPE;
    private SpatialIndexFactory.IndexType INDEX_TYPE;
    private String subscriptionLocation;

    private List<Geometry> subscriptions;
    private GeometryJSON gj;
    SpatialPartitioner partitioner;
    ArrayList<SpatialIndex> partitionedIndex;

    public KafkaGeoIndexBolt(TopologyConfig topologyConfig) {

        System.out.println("\n\n\nTopologyConfig: " + topologyConfig.toString() + "\n\n\n");

        NUMBER_OF_PARTITIONS = topologyConfig.getPartitions();
        DECIMALS = topologyConfig.getDecimals();
        GRID_TYPE = topologyConfig.getPartitionType();
        INDEX_TYPE = topologyConfig.getIndexType();
        subscriptionLocation = topologyConfig.getSubscriptionLocation();

        System.out.println("\n\n\nsubscriptionLocation: " + subscriptionLocation + "\n\n\n");

    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        Stream<String> lines = null;
        try {
            System.out.println("\n\n\nprocessedPublications: " + processedPublications + "\n\n\n");
            System.out.println("\n\n\nprocessingTime: " + processingTime + "\n\n\n");
            System.out.println("\n\n\nSubscription path: " + subscriptionLocation + "\n\n\n");
            Path path = Paths.get(subscriptionLocation);
            System.out.println(path);
            lines = Files.lines(path);
        } catch (IOException | NullPointerException e) {
            System.err.println("\n\n\nNesto Path " + subscriptionLocation + " jebe\n\n\n");
            e.printStackTrace();
        }

        subscriptions = new LinkedList<>();
        gj = new GeometryJSON(19);

        AtomicInteger test = new AtomicInteger();
        //parse and add subscriptions to list
        lines.map(unchecked(line -> gj.read(line))).forEach(geometry -> subscriptions.add(geometry));
        System.out.println("Storm - Number of added subscriptions: " + subscriptions.size());

        //create list of subscription envelopes
        List<Envelope> subscriptionEnvelopes = new LinkedList<>();
        for (Geometry subscription : subscriptions) {
            subscriptionEnvelopes.add(subscription.getEnvelopeInternal());
        }

        //create a partitioner using subscriptions envelopes
        try {
            partitioner = SpatialPartitionerFactory.create(GRID_TYPE, NUMBER_OF_PARTITIONS, subscriptionEnvelopes);
        } catch (Exception e) {
            e.printStackTrace();
        }

        //create a partitioned index (i.e. a spatial index for each partition)
        partitionedIndex = PartitionedSpatialIndexFactory.create(INDEX_TYPE, subscriptions, partitioner);
    }

    @Override
    public void execute(Tuple tuple) {

        if (tuple.getValue(0) != null) {
            LOG.info("--> " + this.getClass().getName() + " received tuple " + tuple.toString());
            // expected tuple structure: type:GeoType, geometry:Geometry, decimals:int

            KafkaTuple kafkaTuple = new KafkaTuple(tuple);

            // begin processing
            long startTime = System.currentTimeMillis();

            GeometryJSON gj = new GeometryJSON(DECIMALS);
            Geometry publication = null;
            try {
                publication = gj.read(kafkaTuple.getValue());

                ConcurrentMap matchingPairs = new ConcurrentHashMap();
                AtomicInteger subscriptionCounter = new AtomicInteger(-1);

                Iterator<Tuple2<Integer, Geometry>> iterator = partitioner.placeObject(publication);
                Geometry finalPublication = publication;
                iterator.forEachRemaining(pair -> {
                    SpatialIndex subscriptionIndex = partitionedIndex.get(pair._1);

                    List<Geometry> matchingSubscriptions = subscriptionIndex.query(finalPublication.getEnvelopeInternal());

                    for (Geometry matchingSubscription : matchingSubscriptions) {
                        if (matchingSubscription.covers(finalPublication)) {
                            matchingPairs.put(matchingSubscription, finalPublication);
                        }
                    }

                    subscriptionCounter.set(matchingPairs.size());
                });

                long endTime = System.currentTimeMillis() - startTime;
                processedPublications++;
                processingTime += endTime;

                tupleCounter++;
                String pubStr = publication.toString() + "\n";
                String matchedStr = tupleCounter + ": Publication matched " + subscriptionCounter + " subscriptions\n";
                String avgProcTime = "Average processing time: " + processingTime / processedPublications + " milis\n";

                collector.emit(new Values(kafkaTuple.getKey(), pubStr +  matchedStr + avgProcTime));
                collector.ack(tuple);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("key", "message"));
    }
}
