/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hr.fer.retrofit.geofil.loading;

import hr.fer.retrofit.geofil.wrapping.AIdedPoint;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * @author Krešimir Pripužić <kresimir.pripuzic@fer.hr>
 */
public class AccidentDataLoader {

    public static List<AIdedPoint> loadCsvFile(String filePath) throws FileNotFoundException, IOException {

        GeometryFactory gf = new GeometryFactory(new PrecisionModel(), 4326);
        List<AIdedPoint> result = null;

        try (Stream<String> stream = Files.lines(Paths.get(filePath))) {
            // skip first line
            result = stream.map(line -> line.split(",")).
                    filter(splittedLine -> isSpittedLineValid(splittedLine)).
                    map(splittedLine -> new AIdedPoint(gf.createPoint(new Coordinate(Double.parseDouble(splittedLine[3]),
                            Double.parseDouble(splittedLine[4]))), splittedLine[0])).
                    collect(Collectors.toList());
        }

        return result;
    }

    private static boolean isSpittedLineValid(String[] splittedLine) {
        try {
            Double.parseDouble(splittedLine[3]);
            Double.parseDouble(splittedLine[4]);
            return true;
        } catch (NumberFormatException ex) {
            return false;
        }
    }
}
