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
import storm.tuples.GeoType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static storm.util.ThrowingFunction.unchecked;

public class GeoIndexBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(GeoIndexBolt.class);

    private OutputCollector collector;
    private int tupleCounter;

    // timer variables
    private long processedPublications = 0;
    private long processingTime = 0;

    private static final int NUMBER_OF_PARTITIONS = 20;
    private static final GridType GRID_TYPE = GridType.VORONOI;

    private List<Geometry> subscriptions;
    private GeometryJSON gj;
    SpatialPartitioner partitioner;
    ArrayList<SpatialIndex> partitionedIndex;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        Stream<String> lines = null;
        try {
            lines = Files.lines(Paths.get("subscriptions19.json"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        subscriptions = new LinkedList<>();
        gj = new GeometryJSON(19);

        //parse and add subscriptions to list
        lines.map(unchecked(line -> gj.read(line))).forEach(geometry
                -> subscriptions.add(geometry));
        System.out.println("Number of added subscriptions: " + subscriptions.size());

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
        partitionedIndex = PartitionedSpatialIndexFactory.create(SpatialIndexFactory.IndexType.STR_TREE, subscriptions, partitioner);
    }

    @Override
    public void execute(Tuple tuple) {

        if (tuple.getValue(0) != null) {
            LOG.info("--> " + this.getClass().getName() + " received tuple " + tuple.toString());
            // expected tuple structure: type:GeoType, geometry:Geometry, decimals:int

            String typeString = ((String) tuple.getValue(0)).toUpperCase();
            GeoType type = GeoType.valueOf(typeString);

            int decimals = Integer.parseInt(tuple.getValue(2).toString());

            // begin processing
            long startTime = System.currentTimeMillis();

            GeometryJSON gj = new GeometryJSON(decimals);
            Geometry publication = null;
            try {
                publication = gj.read(tuple.getValue(1));

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

                collector.emit(new Values(pubStr, matchedStr + ", " + avgProcTime));

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //outputFieldsDeclarer.declare(new Fields("publication", "matched", "avgProcessingTime"));
        outputFieldsDeclarer.declare(new Fields("key", "message"));
    }
}
