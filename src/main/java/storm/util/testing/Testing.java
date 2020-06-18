package storm.util.testing;

import storm.util.TopologyConfig;

import java.io.IOException;

public class Testing {

    public static void main(String[] args) {

        try{
            TopologyConfig config = TopologyConfig.create("config.yaml");
            System.out.println(config.toString());
        } catch (IOException e) {
            System.out.println("File config.yaml cannot be read of found or something else is wrong");
        }

    }
}
