package storm.util.testing;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.time.temporal.ChronoField;

import static java.lang.Thread.sleep;

public class Testing {
    private static int SIX_MIN = 360000;

    private static FileWriter progressWriter;

    public static void main(String[] args) throws IOException {
        progressWriter = new FileWriter("progress.txt");

        writeProgress(getCurrentTime() + " Script started\n");

        //            runExperiment("EQUALGRID", "STR_TREE", 1, SIX_MIN);
//            runExperiment("EQUALGRID", "STR_TREE", 2, SIX_MIN);
        runExperiment("EQUALGRID", "STR_TREE", 4, SIX_MIN);
//            runExperiment("EQUALGRID", "STR_TREE", 8, SIX_MIN);
//            runExperiment("EQUALGRID", "STR_TREE", 16, SIX_MIN);
//            runExperiment("EQUALGRID", "STR_TREE", 32, SIX_MIN);
//            runExperiment("EQUALGRID", "STR_TREE", 64, SIX_MIN);
//            runExperiment("EQUALGRID", "QUAD_TREE", 1, SIX_MIN);
//            runExperiment("EQUALGRID", "QUAD_TREE", 2, SIX_MIN);
//            runExperiment("EQUALGRID", "QUAD_TREE", 4, SIX_MIN);
//            runExperiment("EQUALGRID", "QUAD_TREE", 8, SIX_MIN);
//            runExperiment("EQUALGRID", "QUAD_TREE", 16, SIX_MIN);
//            runExperiment("EQUALGRID", "QUAD_TREE", 32, SIX_MIN);
//            runExperiment("EQUALGRID", "QUAD_TREE", 64, SIX_MIN);

        writeProgress(getCurrentTime() + " Script ended\n");
    }

    private static  void runExperiment(String partitioningType, String indexType, int partitionNumber, int timeout) {
        String experimentName = partitioningType + " " + indexType + " " + partitionNumber;

        try {
            writeProgress(getCurrentTime() + " " + experimentName + " started, timeout= " + milisToMin(timeout) + "seconds\n");

            String topology =
                    String.format("storm jar Geofil_storm-1.0-SNAPSHOT.jar storm.topologies.KafkaGeofilTopology config_storm_cluster_big.yaml %s %s %d", partitioningType, indexType, partitionNumber);
            Runtime rt = Runtime.getRuntime();
            Process topologyProcess = rt.exec(topology);

            BufferedReader input = new BufferedReader(new InputStreamReader(topologyProcess.getInputStream()));
            String line=null;
            while((line=input.readLine()) != null) {
                System.out.println(line);
            }

            sleep(30000); //30 seconds

            writeProgress(getCurrentTime() + " started producer\n");
            String producer = "java -cp GeofilKafkaProducerConsumer-1.0-SNAPSHOT.jar kafka.PublicationsProducer config_kafka_cluster.yaml";
            Process producerProcess = rt.exec(producer);

            input = new BufferedReader(new InputStreamReader(producerProcess.getInputStream()));
            line=null;
            while((line=input.readLine()) != null) {
                System.out.println(line);
            }

            String filename = "cluster-" + partitioningType + "-" + indexType + "-" + partitionNumber + ".txt";
            String consumer =
                    String.format("java -cp GeofilKafkaProducerConsumer-1.0-SNAPSHOT.jar kafka.PublicationsConsumer config_kafka_cluster.yaml %s", filename);
            writeProgress(getCurrentTime() + " started consumer\n");
            Process consumerProcess = Runtime.getRuntime().exec(consumer);
            input = new BufferedReader(new InputStreamReader(consumerProcess.getInputStream()));
            line=null;
            while((line=input.readLine()) != null) {
                System.out.println(line);
            }

            sleep(timeout);

            writeProgress(getCurrentTime() + " killing experiment processes\n");
            // storm kill geofil-storm-EQUALGRID-STR_TREE-4 -w 0
            String topologyName = String.format("geofil-storm-%s-%s-%d", partitioningType, indexType, partitionNumber);
            String killTopology = String.format("storm kill %s -w 0", topologyName);
            System.out.println(killTopology);
            rt.exec(killTopology);
            consumerProcess.destroy();

            writeProgress(getCurrentTime() + " " + experimentName + " finished\n");
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static String getCurrentTime() {
        LocalDateTime now = LocalDateTime.now();
        int year = now.getYear();
        int month = now.getMonthValue();
        int day = now.getDayOfMonth();
        int hour = now.getHour();
        int minute = now.getMinute();
        int second = now.getSecond();
        int millis = now.get(ChronoField.MILLI_OF_SECOND); // Note: no direct getter available.

        return String.format("%d-%02d-%02d %02d:%02d:%02d.%03d", year, month, day, hour, minute, second, millis);
    }

    private static double milisToMin(int milis) {
        return milis * 0.001;
    }

    private static void writeProgress(String line) throws IOException {
        System.out.println(line);
        progressWriter.write(line);
        progressWriter.flush();
    }
}
