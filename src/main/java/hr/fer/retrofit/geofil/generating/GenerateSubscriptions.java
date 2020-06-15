/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hr.fer.retrofit.geofil.generating;

import hr.fer.retrofit.geofil.wrapping.CodedMultiPolygon;
import org.geotools.geojson.geom.GeometryJSON;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;

import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static hr.fer.retrofit.geofil.loading.PostcodeDataLoader.loadShapeFile;
import static hr.fer.retrofit.geofil.processing.ReplicatingProcessor.ThrowingFunction.unchecked;

/**
 *
 * @author Krešimir Pripužić <kresimir.pripuzic@fer.hr>
 */
public class GenerateSubscriptions {

    private static final double AREA_PERCENTAGE = 0.1;
    private static final double DISTRICT_PERCENTAGE = 0.2;
    //SECTOR_PERCENTAGE is 1 - (AREA_PERCENTAGE + DISTRICT_PERCENTAGE)

    private static final int NUMBER_OF_SUBSCRIPTIONS = 1000;

    public static void main(String[] args) throws IOException {
        //load areas
        String filePath = "GB_Postcodes-QGIS_fixed/PostalArea_fixed.shp";
        List<CodedMultiPolygon> areas = loadShapeFile(filePath);
        System.out.println("Areas: " + areas.size());

        //load districts
        filePath = "GB_Postcodes-QGIS_fixed/PostalDistrict_fixed.shp";
        List<CodedMultiPolygon> districts = loadShapeFile(filePath);
        System.out.println("Districts: " + districts.size());

        //load sectors
        filePath = "GB_Postcodes-QGIS_fixed/PostalSector_fixed.shp";
        List<CodedMultiPolygon> sectors = loadShapeFile(filePath);
        System.out.println("Sectors: " + sectors.size());

        //create result array
        Geometry[] geometries = new Geometry[NUMBER_OF_SUBSCRIPTIONS];
        int geometryCounter = 0;
        
        //generate subscriptions
        int maxDecimalPlaces = 0;
        for (int i = 0; i < NUMBER_OF_SUBSCRIPTIONS; i++) {
            Random random = new Random();

            double type = random.nextDouble();
            CodedMultiPolygon polygon;

            if (type < AREA_PERCENTAGE) {
                //generate area subscription
                polygon = areas.get(random.nextInt(areas.size()));
                geometries[geometryCounter++] = polygon.getMultiPolygon();

            } else if (type < DISTRICT_PERCENTAGE) {
                //generate district subsctiption
                polygon = districts.get(random.nextInt(districts.size()));
                geometries[geometryCounter++] = polygon.getMultiPolygon();
            } else {
                //generate sector subscription
                polygon = sectors.get(random.nextInt(sectors.size()));
                geometries[geometryCounter++] = polygon.getMultiPolygon();
            }

            //get decimal digits
            for (Coordinate coordinate : polygon.getMultiPolygon().getCoordinates()) {
                double number = coordinate.x;
                String text = Double.toString(Math.abs(number));
                int decimalPlaces = text.length() - text.indexOf('.') - 1;
                if (decimalPlaces > maxDecimalPlaces) {
                    maxDecimalPlaces = decimalPlaces;
                }
            }

        }        

        //write shapefile
//        GeometryCollection subscriptions = new GeometryCollection(geometries, new GeometryFactory());
//        FileOutputStream shp = new FileOutputStream("Subscriptions/Subscriptions.shp");
//        FileOutputStream shx = new FileOutputStream("Subscriptions/Subscriptions.shx");
//        try (ShapefileWriter writer = new ShapefileWriter(shp.getChannel(), shx.getChannel())) {
//            writer.write(subscriptions, ShapeType.POLYGON);
//        }

        //write json collection
//        GeometryJSON geom = new GeometryJSON();
//        geom.writeGeometryCollection(subscriptions, "subscriptions.json");
        
        //write one json per line
        GeometryJSON gj = new GeometryJSON(maxDecimalPlaces);
        try (FileWriter fw = new FileWriter("subscriptions" + maxDecimalPlaces + ".json")) {
            for (Geometry geometry : geometries) {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                gj.write(geometry, bos);
                fw.append(bos.toString() + "\n");
            }
        }

        //load json subscriptions from disk
        Stream<String> lines = Files.lines(Paths.get("subscriptions" + maxDecimalPlaces + ".json"));
        List<Geometry> jsonGeometries = new LinkedList<>();

        //parse and add subscriptions to list
        lines.map(unchecked(line -> gj.read(line))).forEach(geometry
                -> jsonGeometries.add(geometry));
        System.out.println("Number of loaded subscriptions: " + jsonGeometries.size());

        int counter = 0;
        for (int i = 0; i < NUMBER_OF_SUBSCRIPTIONS; i++) {
            if (!geometries[i].equals(jsonGeometries.get(i))) {
                counter++;
            }
        }
        
        System.out.println("Number of unequal subscriptions: " + counter);
    }
}
