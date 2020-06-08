/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hr.fer.retrofit.geofil.generating;

import hr.fer.retrofit.geofil.loading.AccidentDataLoader;
import hr.fer.retrofit.geofil.wrapping.AIdedPoint;
import hr.fer.retrofit.geofil.wrapping.CodedMultiPolygon;
import org.geotools.geojson.geom.GeometryJSON;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.strtree.STRtree;

import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

import static hr.fer.retrofit.geofil.loading.PostcodeDataLoader.loadShapeFile;
import static hr.fer.retrofit.geofil.processing.ReplicatingProcessor.ThrowingFunction.unchecked;

/**
 *
 * @author Krešimir Pripužić <kresimir.pripuzic@fer.hr>
 */
public class GeneratePublications {

    private static final double AREA_PERCENTAGE = 0.1;
    private static final double DISTRICT_PERCENTAGE = 0.2;
    private static final double SECTOR_PERCENTAGE = 0.3;
    //POINT_PERCENTAGE is 1 - (AREA_PERCENTAGE + DISTRICT_PERCENTAGE + SECTOR_PERCENTAGE)

    private static final int NUMBER_OF_PUBLICATIONS = 1000;

    public static void main(String[] args) throws IOException {
        //load areas
        String filePath = "GB_Postcodes-QGIS_fixed/PostalArea_fixed.shp";
        List<CodedMultiPolygon> areas = loadShapeFile(filePath);
        System.out.println("Areas: " + areas.size());

        STRtree areaIndex = new STRtree();
        for (CodedMultiPolygon area : areas) {
            areaIndex.insert(area.getMultiPolygon().getEnvelopeInternal(), area);
        }
        areaIndex.build();

        //load districts
        filePath = "GB_Postcodes-QGIS_fixed/PostalDistrict_fixed.shp";
        List<CodedMultiPolygon> districts = loadShapeFile(filePath);
        System.out.println("Districts: " + districts.size());

        STRtree districtIndex = new STRtree();
        for (CodedMultiPolygon district : districts) {
            districtIndex.insert(district.getMultiPolygon().getEnvelopeInternal(), district);
        }
        districtIndex.build();

        //load sectors
        filePath = "GB_Postcodes-QGIS_fixed/PostalSector_fixed.shp";
        List<CodedMultiPolygon> sectors = loadShapeFile(filePath);
        System.out.println("Sectors: " + sectors.size());

        STRtree sectorIndex = new STRtree();
        for (CodedMultiPolygon sector : sectors) {
            sectorIndex.insert(sector.getMultiPolygon().getEnvelopeInternal(), sector);
        }
        sectorIndex.build();

        //load points
        filePath = "dft-accident-data/Accidents0515.csv";
        List<AIdedPoint> codedPoints = AccidentDataLoader.loadCsvFile(filePath);
        System.out.println("Points: " + codedPoints.size());

        //create results array
        Geometry[] polygons = new Geometry[NUMBER_OF_PUBLICATIONS];
        int polygonCounter = 0;
        Geometry[] points = new Geometry[NUMBER_OF_PUBLICATIONS];
        int pointCounter = 0;

        //generate publications
        int maxDecimalPlaces = 0;
        for (int i = 0; i < NUMBER_OF_PUBLICATIONS; i++) {
            Random random = new Random();
            AIdedPoint point = codedPoints.get(random.nextInt(codedPoints.size()));

            List<CodedMultiPolygon> matchingAreas = areaIndex.query(point.getPoint().getEnvelopeInternal());
            List<CodedMultiPolygon> matchingDistricts = districtIndex.query(point.getPoint().getEnvelopeInternal());
            List<CodedMultiPolygon> matchingSectors = sectorIndex.query(point.getPoint().getEnvelopeInternal());

            int counter = 0;
            for (CodedMultiPolygon matchingArea : matchingAreas) {
                if (matchingArea.getMultiPolygon().covers(point.getPoint())) {
                    counter++;
                    break;
                }
            }
            for (CodedMultiPolygon matchingDistrict : matchingDistricts) {
                if (matchingDistrict.getMultiPolygon().covers(point.getPoint())) {
                    counter++;
                    break;
                }
            }
            for (CodedMultiPolygon matchingSector : matchingSectors) {
                if (matchingSector.getMultiPolygon().covers(point.getPoint())) {
                    counter++;
                    break;
                }
            }
            if (counter == 3) {
                //point is valid
                double type = random.nextDouble();

                Geometry geometry = null;
                if (type < AREA_PERCENTAGE) {
                    //generate area publication
                    for (CodedMultiPolygon matchingArea : matchingAreas) {
                        if (matchingArea.getMultiPolygon().covers(point.getPoint())) {
                            geometry = matchingArea.getMultiPolygon();
                            polygons[polygonCounter++] = geometry;
                            break;
                        }
                    }

                } else if (type < DISTRICT_PERCENTAGE) {
                    //generate district publication
                    for (CodedMultiPolygon matchingDistrict : matchingDistricts) {
                        if (matchingDistrict.getMultiPolygon().covers(point.getPoint())) {
                            geometry = matchingDistrict.getMultiPolygon();
                            polygons[polygonCounter++] = geometry;
                            break;
                        }
                    }
                } else if (type < SECTOR_PERCENTAGE) {
                    //generate sector publication
                    for (CodedMultiPolygon matchingSector : matchingSectors) {
                        if (matchingSector.getMultiPolygon().covers(point.getPoint())) {
                            geometry = matchingSector.getMultiPolygon();
                            polygons[polygonCounter++] = geometry;
                            break;
                        }
                    }
                } else {
                    //generate point publication
                    geometry = point.getPoint();
                    points[pointCounter++] = geometry;
                }

                //get decimal digits
                for (Coordinate coordinate : geometry.getCoordinates()) {
                    double number = coordinate.x;
                    String text = Double.toString(Math.abs(number));
                    int decimalPlaces = text.length() - text.indexOf('.') - 1;
                    if (decimalPlaces > maxDecimalPlaces) {
                        maxDecimalPlaces = decimalPlaces;
                    }
                }
            } else {
                //invalid point;
                i--;
            }
        }

        //write shapefile for polygons
//        FileOutputStream shp = new FileOutputStream("Publications/Polygons.shp");
//        FileOutputStream shx = new FileOutputStream("Publications/Polygons.shx");
//        ShapefileWriter writer = new ShapefileWriter(shp.getChannel(), shx.getChannel());
//        GeometryCollection publications = new GeometryCollection(Arrays.copyOfRange(polygons, 0, polygonCounter), new GeometryFactory());
//        writer.write(publications, ShapeType.POLYGON);

        //write shapefile for points
//        shp = new FileOutputStream("Publications/Points.shp");
//        shx = new FileOutputStream("Publications/Points.shx");
//        writer = new ShapefileWriter(shp.getChannel(), shx.getChannel());
//        publications = new GeometryCollection(Arrays.copyOfRange(points, 0, pointCounter), new GeometryFactory());
//        writer.write(publications, ShapeType.POINT);
//        writer.close();

        //write json collection
//            GeometryJSON geom = new GeometryJSON();
//            Geometry[] geometries = Arrays.copyOf(polygons, polygonCounter + pointCounter);
//            System.arraycopy(points, 0, geometries, polygonCounter, pointCounter);
//            publications = new GeometryCollection(geometries, new GeometryFactory());
//            geom.writeGeometryCollection(publications, "publications.json");
        
        //write one json per line
        Geometry[] geometries = Arrays.copyOf(polygons, polygonCounter + pointCounter);
        System.arraycopy(points, 0, geometries, polygonCounter, pointCounter);
        List<Geometry> shuffledGeometries = Arrays.asList(geometries);
        Collections.shuffle(shuffledGeometries);

        GeometryJSON gj = new GeometryJSON(maxDecimalPlaces);
        try (FileWriter fw = new FileWriter("publications" + maxDecimalPlaces + ".json")) {
            for (Geometry geometry : shuffledGeometries) {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                gj.write(geometry, bos);
                fw.append(bos.toString() + "\n");
            }
        }
        
        //load json publications from disk
        Stream<String> lines = Files.lines(Paths.get("publications" + maxDecimalPlaces + ".json"));
        List<Geometry> jsonGeometries = new LinkedList<>();

        //parse and add subscriptions to list
        lines.map(unchecked(line -> gj.read(line))).forEach(geometry
                -> jsonGeometries.add(geometry));
        System.out.println("Number of loaded publications: " + jsonGeometries.size());

        int counter = 0;
        for (int i = 0; i < NUMBER_OF_PUBLICATIONS; i++) {
            if (!geometries[i].equals(jsonGeometries.get(i))) {
                counter++;
            }
        }
        
        System.out.println("Number of unequal publications: " + counter);
    }
}
