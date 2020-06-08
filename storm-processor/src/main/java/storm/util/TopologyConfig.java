package storm.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import hr.fer.retrofit.geofil.indexing.SpatialIndexFactory;
import org.codehaus.jackson.annotate.JsonProperty;
import org.datasyslab.geospark.enums.GridType;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

public class TopologyConfig {

    @JsonProperty
    GridType gridType;
    SpatialIndexFactory.IndexType indexType;

    public static TopologyConfig create() {
        String dir = System.getProperty("user.dir");
        File config = Paths.get(dir + File.separator + "config.yaml").toFile();

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        TopologyConfig topologyConfig = null;
        try {
            topologyConfig = mapper.readValue(config, TopologyConfig.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

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
}
