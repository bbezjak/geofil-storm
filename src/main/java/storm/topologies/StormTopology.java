package storm.topologies;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import storm.bolts.FileWriterBolt;
//import storm.bolts.GeoIndexBoltJts;
import storm.bolts.GeoIndexBolt;
import storm.spouts.FileReadSpout;

public class StormTopology {

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setDebug(false);

        TopologyBuilder builder = new TopologyBuilder();

        String topologyName = "geo-indexing-storm";
        if (args != null && args.length > 0) {
            topologyName = args[0];
        }

        buildTopologySequential(conf, builder);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, builder.createTopology());

//        try {
//            StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
//        }
//        catch(Exception ex){
//            ex.printStackTrace();
//        }
    }

    private static void buildTopologySequential(Config conf, TopologyBuilder builder) {
        builder.setSpout("spout", new FileReadSpout(), 1);

//        builder.setBolt("indexer", new GeoIndexBoltGeoSpark("subscriptions19.json"), 1).shuffleGrouping("spout");
        builder.setBolt("indexer", new GeoIndexBolt(), 5).shuffleGrouping("spout");

        builder.setBolt("count", new FileWriterBolt("results.txt"), 5).shuffleGrouping("indexer");

        conf.setNumWorkers(3);
    }

//    private static void buildTopologySequential(Config conf, TopologyBuilder builder) {
//        builder.setSpout("spout", new FileReadSpout(), 1);
//
////        builder.setBolt("indexer", new GeoIndexBoltGeoSpark("subscriptions19.json"), 1).shuffleGrouping("spout");
//        builder.setBolt("indexer", new GeoIndexBoltJts("subscriptions19.json"), 1).shuffleGrouping("spout");
//
//        builder.setBolt("count", new FileWriterBolt("results.txt"), 1).shuffleGrouping("indexer");
//
//        conf.setNumWorkers(3);
//    }
//
//    private static void buildTopologyWithDistributedIndex(Config conf, TopologyBuilder builder) {
//        builder.setSpout("spout", new FileReadSpout(), 1);
//
////        builder.setBolt("indexer", new GeoIndexBoltGeoSpark(), 1).shuffleGrouping("spout");
//        builder.setBolt("indexer1", new GeoIndexBoltJts("partial_files/subscriptions19_1.json"), 1).shuffleGrouping("spout");
//        builder.setBolt("indexer2", new GeoIndexBoltJts("partial_files/subscriptions19_2.json"), 1).shuffleGrouping("spout");
//        builder.setBolt("indexer3", new GeoIndexBoltJts("partial_files/subscriptions19_3.json"), 1).shuffleGrouping("spout");
//        builder.setBolt("indexer4", new GeoIndexBoltJts("partial_files/subscriptions19_4.json"), 1).shuffleGrouping("spout");
//
//        builder.setBolt("count", new FileWriterBolt("results.txt"), 1).shuffleGrouping("indexer");
//
//        conf.setNumWorkers(6);
//    }
}
