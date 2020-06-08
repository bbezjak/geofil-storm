import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.geotools.geojson.geom.GeometryJSON;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

public class PublicationsConsumer {

    private static int decimals = 19;

    public static void main(String args[]) {
        final KafkaStreams streams = createStreams("localhost:9092", "/tmp/kafka-streams");

        streams.cleanUp();

        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static KafkaStreams createStreams(final String bootstrapServers,
                                              final String stateDir) {

        final Properties streamsConfiguration = new Properties();
        // unique app id on kafka cluster
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "geofill-data-storm.util.testing");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "cgeofill-data-storm.util.testing-client");
        // kafka broker address
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // local state store
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        // consumer from the beginning of the topic or last offset
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // override default serdes
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        // Get the stream of station statuses
        GeometryJSON gj = new GeometryJSON(decimals);
        KStream<String, String> geometryStream = builder.stream(
                "geofil_results",
                Consumed.with(Serdes.String(), Serdes.String()))
                .map((geometry_id, stringValue) -> {
                    try {
                        //Geometry geometry = gj.read(stringValue);
                        return new KeyValue<>(geometry_id, stringValue);
                    } catch (Exception e) {
                        throw new RuntimeException("Deserialize error" + e);
                    }
                });

        FileWriter fw = null;
        try {
            fw = new FileWriter("kafka_output.txt", true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        FileWriter finalFw = fw;
        geometryStream.foreach(new ForeachAction<String, String>() {
            @Override
            public void apply(String s, String s2) {
                try {
                    finalFw.append(s + ": Geometry " + s2 + "\n");
                    System.out.println(s + "\n");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

        return new KafkaStreams(builder.build(), streamsConfiguration);
    }
}