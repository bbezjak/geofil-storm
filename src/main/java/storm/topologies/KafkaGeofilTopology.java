package storm.topologies;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import storm.bolts.KafkaGeoIndexBolt;
import storm.util.TopologyConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.stream.Stream;

import static storm.util.ThrowingFunction.unchecked;

public class KafkaGeofilTopology {

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setDebug(false);

        TopologyBuilder builder = new TopologyBuilder();

        String topologyName = "geofil-storm-kafka";
        if (args != null && args.length > 0) {
            topologyName = args[0];
        }

        TopologyConfig topologyConfig;
        try {
            topologyConfig = TopologyConfig.create();
        } catch (IOException e) {
            throw new RuntimeException("Failed to read configuration");
        }

        // propsi se mogu napisati i s ProducerConfig klasom, vidi kafka-client-examples u stormu
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaSpout kafkaSpout =
                new KafkaSpout<>(
                        KafkaSpoutConfig.builder("127.0.0.1:9092", "geofil_publications")
                                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "kafkaSpout")
                                .build());


        // Potrebno je implementirati sučelje TuppleToKafkaMapper s dvije metode:
        // K getKeyFromTuple(Tuple/TridentTuple tuple);
        // V getMessageFromTuple(Tuple/TridentTuple tuple);
        //
        // S FieldNameBasedTupleToKafkaMapper implementacijom moram koristiti
        // jedan filed kao key i jedan kao value.
        //
        // FieldNameBasedTupleToKafkaMapper defaultno traži da se fieldovi zovu "key" i "message", ali to je moguće
        // i overridati u konstruktoru (samo pukneš 2 parametra u new FieldNameBasedTupleToKafkaMapper()).
        // To ostvariš u boltu prije koji šalje podatke u takvom obliku

        KafkaBolt kafkaBolt = new KafkaBolt<>()
                .withProducerProperties(props)
                .withTopicSelector(new DefaultTopicSelector("geofil_results"))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());

        builder.setSpout("kafkaSpout", kafkaSpout, 1);
        builder.setBolt("geoIndexBolt", new KafkaGeoIndexBolt(topologyConfig), 1).shuffleGrouping("kafkaSpout");
        builder.setBolt("kafkaBolt", kafkaBolt, 1).shuffleGrouping("geoIndexBolt");

        if(topologyConfig.isLocal()) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, conf, builder.createTopology());
        } else {
            try {
                StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
            }
            catch(Exception ex){
                ex.printStackTrace();
            }
        }
    }
}
