package storm.bolts;

import hr.fer.retrofit.geofil.indexing.PartitionedSpatialIndexFactory;
import hr.fer.retrofit.geofil.indexing.SpatialIndexFactory;
import hr.fer.retrofit.geofil.partitioning.SpatialPartitionerFactory;
import org.apache.storm.Config;
import org.apache.storm.blobstore.AtomicOutputStream;
import org.apache.storm.blobstore.BlobStoreAclHandler;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.blobstore.InputStreamWithMeta;
import org.apache.storm.generated.*;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner;
import org.geotools.geojson.geom.GeometryJSON;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.SpatialIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import storm.util.DataLocality;
import storm.util.KafkaTuple;
import storm.util.TopologyConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import static storm.util.ThrowingFunction.unchecked;

public class KafkaGeoIndexBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(GeoIndexBolt.class);

    private OutputCollector collector;
    private int tupleCounter;

    // timer variables
    private long processedPublications = 0;
    private long processingTime = 0;

    private List<Geometry> subscriptions;
    private GeometryJSON gj;
    private SpatialPartitioner partitioner;
    private ArrayList<SpatialIndex> partitionedIndex;

    GridType gridType;
    SpatialIndexFactory.IndexType indexType;
    int partitionsNumber;

    String kafkaInBroker;
    String kafkaInTopic;

    String kafkaOutBroker;
    String kafkaOutTopic;

    DataLocality dataLocality;
    String sdcaKey;
    String subscriptionLocation;

    int decimals;

    public KafkaGeoIndexBolt(TopologyConfig topologyConfig) {
        gridType = topologyConfig.getGridType();
        indexType = topologyConfig.getIndexType();
        partitionsNumber = topologyConfig.getPartitionsNumber();
        kafkaInBroker = topologyConfig.getKafkaInBroker();
        kafkaInTopic = topologyConfig.getKafkaInTopic();
        kafkaOutBroker = topologyConfig.getKafkaOutBroker();
        kafkaOutTopic = topologyConfig.getKafkaOutTopic();
        dataLocality = topologyConfig.getDataLocality();
        sdcaKey = topologyConfig.getSdcaKey();
        subscriptionLocation = topologyConfig.getSubscriptionLocation();
        decimals = topologyConfig.getDecimals();
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        Stream<String> lines = null;
        try {
            if(dataLocality == DataLocality.LOCAL) {
                System.out.println("Reading from local " + subscriptionLocation);
                Path path = Paths.get(subscriptionLocation);
                lines = Files.lines(path);
            } else if(dataLocality == DataLocality.HDFS) {
                System.out.println("Reading from hdfs " + subscriptionLocation);
                Configuration conf = new Configuration();
                conf.addResource(new org.apache.hadoop.fs.Path("/etc/hadoop/conf/core-site.xml"));
                conf.addResource(new org.apache.hadoop.fs.Path("/etc/hadoop/conf/hdfs-site.xml"));
                FileSystem hdfs = FileSystem.get(new URI("hdfs://10.19.8.199:8020"), conf);
                org.apache.hadoop.fs.Path path =
                        new org.apache.hadoop.fs.Path(subscriptionLocation);
                BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(path)));
                List<String> readLines = new LinkedList<>();
                try {
                    String line;
                    line=br.readLine();
                    while (line != null){
                        readLines.add(line);
                        // be sure to read the next line otherwise you'll get an infinite loop
                        line = br.readLine();
                    }
                    System.out.println("\n\nRead subscriptions from HDFS: " + readLines.size() + "\n\n");
                    lines = readLines.stream();
                } finally {
                    // you should close out the BufferedReader
                    br.close();
                }
            } else if(dataLocality == DataLocality.SDCA) {
                System.out.println("Reading from Storm Distributed Cache API, blob key" + sdcaKey);
                Config stormConf = new Config();
                stormConf.putAll(Utils.readStormConfig());
                ClientBlobStore clientBlobStore = Utils.getClientBlobStore(stormConf);

                String blobKey = sdcaKey;
                InputStreamWithMeta blobInputStream = clientBlobStore.getBlob(blobKey);
                BufferedReader r = new BufferedReader(new InputStreamReader(blobInputStream));
                List<String> readLines = new LinkedList<>();
                try {
                    String line;
                    line=r.readLine();
                    while (line != null){
                        readLines.add(line);
                        // be sure to read the next line otherwise you'll get an infinite loop
                        line = r.readLine();
                    }
                    System.out.println("\n\nRead subscriptions from SDCA: " + readLines.size() + "\n\n");
                    lines = readLines.stream();
                } finally {
                    // you should close out the BufferedReader
                    r.close();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NullPointerException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        } catch (KeyNotFoundException e) {
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
            partitioner = SpatialPartitionerFactory
                            .create(gridType, partitionsNumber, subscriptionEnvelopes);
        } catch (Exception e) {
            e.printStackTrace();
        }

        //create a partitioned index (i.e. a spatial index for each partition)
        partitionedIndex = PartitionedSpatialIndexFactory.create(indexType, subscriptions, partitioner);
    }

    @Override
    public void execute(Tuple tuple) {

        if (tuple.getValue(0) != null) {
            LOG.info("--> " + this.getClass().getName() + " received tuple " + tuple.toString());
            // expected tuple structure: type:GeoType, geometry:Geometry, decimals:int

            KafkaTuple kafkaTuple = new KafkaTuple(tuple);

            // begin processing
            long startTime = System.currentTimeMillis();

            GeometryJSON gj = new GeometryJSON(decimals);
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
                long avgProcTime = processingTime / processedPublications;
                String avgProcTimeStr = "Average processing time: " + avgProcTime + " milis\n";

                collector.emit(new Values(kafkaTuple.getKey(), avgProcTime));
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
