package storm.util.generating;

import org.geotools.geojson.geom.GeometryJSON;
import org.locationtech.jts.geom.Geometry;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static storm.util.ThrowingFunction.unchecked;

public class GenerateBig {

    public static void main(String args[]) throws IOException {
        // inputFile, outputFile, multiplier

        String inputFile = args[0];
        String outputFile = args[1];
        Integer multiplier = Integer.valueOf(args[2]);

        GeometryJSON gj = new GeometryJSON(19);
        List<Geometry> geometries =
                Files.lines(Paths.get(inputFile))
                        .map(unchecked(line -> gj.read((line))))
                        .collect(Collectors.toList());

        try (FileWriter fw = new FileWriter(outputFile)) {
            for(int i=0; i<multiplier; i++) {
                System.out.println(i + ". time");
                for (Geometry geometry : geometries) {
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    gj.write(geometry, bos);
                    fw.append(bos.toString() + "\n");
                }
            }
        }
    }
}
