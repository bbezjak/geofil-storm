package storm.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import hr.fer.retrofit.geofil.indexing.SpatialIndexFactory;
import org.datasyslab.geospark.enums.GridType;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

public class TopologyConfig {

    GridType gridType;
    SpatialIndexFactory.IndexType indexType;
    int partitionsNumber;

    String kafkaInBroker;
    String kafkaInTopic;

    String kafkaOutBroker;
    String kafkaOutTopic;

    boolean fromHdfs;
    boolean local;
    String subscriptionLocation;

    int decimals;

    public static TopologyConfig create(String path) throws IOException {

        System.out.println("Reading config from " + path);
        File config = Paths.get(path).toFile();

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        TopologyConfig topologyConfig = mapper.readValue(config, TopologyConfig.class);

        System.out.println("Used configuration:");
        System.out.println(topologyConfig.toString());
        return topologyConfig;
    }

    public GridType getGridType() {
        return gridType;
    }

    public void setGridType(GridType gridType) {
        this.gridType = gridType;
    }

    public SpatialIndexFactory.IndexType getIndexType() {
        return indexType;
    }

    public void setIndexType(SpatialIndexFactory.IndexType indexType) {
        this.indexType = indexType;
    }

    public int getPartitionsNumber() {
        return partitionsNumber;
    }

    public void setPartitionsNumber(int partitionsNumber) {
        this.partitionsNumber = partitionsNumber;
    }

    public String getKafkaInBroker() {
        return kafkaInBroker;
    }

    public void setKafkaInBroker(String kafkaInBroker) {
        this.kafkaInBroker = kafkaInBroker;
    }

    public String getKafkaInTopic() {
        return kafkaInTopic;
    }

    public void setKafkaInTopic(String kafkaInTopic) {
        this.kafkaInTopic = kafkaInTopic;
    }

    public String getKafkaOutBroker() {
        return kafkaOutBroker;
    }

    public void setKafkaOutBroker(String kafkaOutBroker) {
        this.kafkaOutBroker = kafkaOutBroker;
    }

    public String getKafkaOutTopic() {
        return kafkaOutTopic;
    }

    public void setKafkaOutTopic(String kafkaOutTopic) {
        this.kafkaOutTopic = kafkaOutTopic;
    }

    public boolean isFromHdfs() {
        return fromHdfs;
    }

    public void setFromHdfs(boolean fromHdfs) {
        this.fromHdfs = fromHdfs;
    }

    public boolean isLocal() {
        return local;
    }

    public void setLocal(boolean local) {
        this.local = local;
    }

    public String getSubscriptionLocation() {
        return subscriptionLocation;
    }

    public void setSubscriptionLocation(String subscriptionLocation) {
        this.subscriptionLocation = subscriptionLocation;
    }

    public int getDecimals() {
        return decimals;
    }

    public void setDecimals(int decimals) {
        this.decimals = decimals;
    }

    @Override
    public String toString() {
        return "TopologyConfig{" +
                "partitionType=" + gridType +
                ", indexType=" + indexType +
                ", partitions=" + partitionsNumber +
                ", kafkaInBroker='" + kafkaInBroker + '\'' +
                ", kafkaInTopic='" + kafkaInTopic + '\'' +
                ", kafkaOutBroker='" + kafkaOutBroker + '\'' +
                ", kafkaOutTopic='" + kafkaOutTopic + '\'' +
                ", fromHdfs=" + fromHdfs +
                ", subscriptionLocation='" + subscriptionLocation + '\'' +
                ", decimals=" + decimals +
                '}';
    }
}
