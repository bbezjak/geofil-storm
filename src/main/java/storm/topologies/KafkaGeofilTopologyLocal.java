package storm.topologies;

import org.apache.storm.LocalCluster;

public class KafkaGeofilTopologyLocal {

    public static void main(String args[]) throws Exception {
        LocalCluster.withLocalModeOverride(() -> {
            KafkaGeofilTopology.main(args);
            return null;
        }, 2000);
    }
}
