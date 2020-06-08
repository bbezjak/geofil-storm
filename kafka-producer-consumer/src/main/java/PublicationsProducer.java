import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.geotools.geojson.geom.GeometryJSON;
import org.locationtech.jts.geom.Geometry;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

public class PublicationsProducer {

    public static final String topicName = "geofil_publications_1";
    public static AtomicInteger seq = new AtomicInteger(0);

    public static void main(String args[]) throws IOException {

        Stream<String> lines = Files.lines(Paths.get("publications19.json"));

//        List<Geometry> publications = new LinkedList<>();
//        GeometryJSON gj = new GeometryJSON(19);

//        //parse and add subscriptions to list
//        lines.map(ThrowingFunction.unchecked(line -> gj.read(line))).forEach(geometry
//                -> publications.add(geometry));
//        System.out.println("Number of added subscriptions: " + publications.size());

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);
        props.put("schema.registry.url", "http://localhost:8081");

        Producer<String, String> producer = new KafkaProducer<>(props);

//        publications.forEach(publication -> {
//            try {
//                String value = publication.toString();
//                int id = seq.incrementAndGet();
//                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, String.valueOf(id), value);
//                System.out.println(record);
//                producer.send(record);
//            } catch (Exception e) {
//                System.out.println("Failed to serialize");
//                throw new RuntimeException(e.getMessage());
//            }
//        });

        lines.forEach(line -> {
            try {
                int id = seq.incrementAndGet();
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, String.valueOf(id), line);
                System.out.println(record);
                producer.send(record);
            } catch (Exception e) {
                System.out.println("Failed to serialize");
                throw new RuntimeException(e.getMessage());
            }
        });

        producer.flush();
        producer.close();
    }

    @FunctionalInterface
    public interface ThrowingFunction<T, R, E extends Throwable> {

        R apply(T t) throws E;

        static <T, R, E extends Throwable> Function<T, R> unchecked(ThrowingFunction<T, R, E> f) {
            return t -> {
                try {
                    return f.apply(t);
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            };
        }
    }
}
