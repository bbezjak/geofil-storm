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

    public static TopologyConfig create() throws IOException {
        String dir = System.getProperty("user.dir");
        File config = Paths.get(dir + File.separator + "config.yaml").toFile();

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        TopologyConfig topologyConfig = mapper.readValue(config, TopologyConfig.class);

        return topologyConfig;
    }

    public TopologyConfig() {
    }

    public TopologyConfig(GridType partitionType,
                          SpatialIndexFactory.IndexType indexType, int partitions, int decimals) {
        this.partitionType = partitionType;
        this.indexType = indexType;
        this.partitions = partitions;
        this.decimals = decimals;
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
}
