package storm.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import hr.fer.retrofit.geofil.indexing.SpatialIndexFactory;
import org.datasyslab.geospark.enums.GridType;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

public class TopologyConfig {

    GridType partitionType;
    SpatialIndexFactory.IndexType indexType;
    int partitions;
    int decimals;
    boolean local;

    String publicationLocation;
    String subscriptionLocation;

    public static TopologyConfig create() throws IOException {
        String dir = System.getProperty("user.dir");
        String path = dir + File.separator + "config.yaml";
        System.out.println("Reading config from " + path);
        File config = Paths.get(path).toFile();

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        TopologyConfig topologyConfig = mapper.readValue(config, TopologyConfig.class);

        System.out.println(topologyConfig.toString());
        return topologyConfig;
    }

    public TopologyConfig() {
    }

    public TopologyConfig(GridType partitionType, SpatialIndexFactory.IndexType indexType, int partitions, int decimals, boolean local, String publicationLocation, String subscriptionLocation) {
        this.partitionType = partitionType;
        this.indexType = indexType;
        this.partitions = partitions;
        this.decimals = decimals;
        this.local = local;
        this.publicationLocation = publicationLocation;
        this.subscriptionLocation = subscriptionLocation;
    }

    public GridType getPartitionType() {
        return partitionType;
    }

    public void setPartitionType(GridType partitionType) {
        this.partitionType = partitionType;
    }

    public SpatialIndexFactory.IndexType getIndexType() {
        return indexType;
    }

    public void setIndexType(SpatialIndexFactory.IndexType indexType) {
        this.indexType = indexType;
    }

    public int getPartitions() {
        return partitions;
    }

    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    public int getDecimals() {
        return decimals;
    }

    public void setDecimals(int decimals) {
        this.decimals = decimals;
    }

    public boolean isLocal() {
        return local;
    }

    public void setLocal(boolean local) {
        this.local = local;
    }

    public String getPublicationLocation() {
        return publicationLocation;
    }

    public void setPublicationLocation(String publicationLocation) {
        this.publicationLocation = publicationLocation;
    }

    public String getSubscriptionLocation() {
        return subscriptionLocation;
    }

    public void setSubscriptionLocation(String subscriptionLocation) {
        this.subscriptionLocation = subscriptionLocation;
    }

    @Override
    public String toString() {
        return "TopologyConfig{" +
                "partitionType=" + partitionType +
                ", indexType=" + indexType +
                ", partitions=" + partitions +
                ", decimals=" + decimals +
                ", local=" + local +
                ", publicationLocation='" + publicationLocation + '\'' +
                ", subscriptionLocation='" + subscriptionLocation + '\'' +
                '}';
    }
}
