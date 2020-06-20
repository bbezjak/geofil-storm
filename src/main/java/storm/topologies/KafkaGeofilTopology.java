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
import java.util.Properties;

public class KafkaGeofilTopology {

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setDebug(true);

        TopologyBuilder builder = new TopologyBuilder();

        TopologyConfig topologyConfig;
        try {
            topologyConfig = TopologyConfig.create(args[0]);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to read configuration");
        }

        String topologyName =
                "geofil-storm-" +
                        topologyConfig.getGridType() + "-" +
                        topologyConfig.getIndexType() + "-" +
                        topologyConfig.getPartitionsNumber();

        KafkaSpout kafkaSpout =
                new KafkaSpout<>(
                        KafkaSpoutConfig.builder(topologyConfig.getKafkaInBroker(), topologyConfig.getKafkaInTopic())
                                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "kafkaSpout")
                                .build());

        // propsi se mogu napisati i s ProducerConfig klasom, vidi kafka-client-examples u stormu
        Properties props = new Properties();
        props.put("bootstrap.servers", topologyConfig.getKafkaOutBroker());
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

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
                .withTopicSelector(new DefaultTopicSelector(topologyConfig.getKafkaOutTopic()))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());

        builder.setSpout("kafkaSpout", kafkaSpout, 1);
        builder.setBolt("geoIndexBolt", new KafkaGeoIndexBolt(topologyConfig), 1).shuffleGrouping("kafkaSpout");
        builder.setBolt("kafkaBolt", kafkaBolt, 1).shuffleGrouping("geoIndexBolt");

        if(topologyConfig.isLocal()) {
            System.out.println("Running topology in local cluster");
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, conf, builder.createTopology());
        } else {
            System.out.println("Submitting topology to Storm cluster");
            try {
                StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
            }
            catch(Exception ex){
                ex.printStackTrace();
            }
        }
    }
}
