/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hr.fer.retrofit.geofil.checking;

import hr.fer.retrofit.geofil.loading.AccidentDataLoader;
import hr.fer.retrofit.geofil.wrapping.AIdedPoint;
import hr.fer.retrofit.geofil.wrapping.CodedMultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.index.strtree.STRtree;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static hr.fer.retrofit.geofil.loading.PostcodeDataLoader.loadShapeFile;

/**
 *
 * @author Krešimir Pripužić <kresimir.pripuzic@fer.hr>
 */
public class CheckAccidentData {

    public static void main(String[] args) throws IOException {
        //load areas
        String filePath = "GB_Postcodes-QGIS_fixed/PostalArea_fixed.shp";
        List<CodedMultiPolygon> areas = loadShapeFile(filePath);

        STRtree areaIndex = new STRtree();
        for (CodedMultiPolygon area : areas) {
            areaIndex.insert(area.getMultiPolygon().getEnvelopeInternal(), area);
        }
        areaIndex.build();

        //load districts
        filePath = "GB_Postcodes-QGIS_fixed/PostalDistrict_fixed.shp";
        List<CodedMultiPolygon> districts = loadShapeFile(filePath);

        STRtree districtIndex = new STRtree();
        for (CodedMultiPolygon district : districts) {
            districtIndex.insert(district.getMultiPolygon().getEnvelopeInternal(), district);
        }
        districtIndex.build();

        //load sectors
        filePath = "GB_Postcodes-QGIS_fixed/PostalSector_fixed.shp";
        List<CodedMultiPolygon> sectors = loadShapeFile(filePath);
        System.out.println("Sectors: " + sectors.size());

        //count sectors covered by districts
        STRtree sectorIndex = new STRtree();
        for (CodedMultiPolygon sector : sectors) {
            sectorIndex.insert(sector.getMultiPolygon().getEnvelopeInternal(), sector);
        }
        sectorIndex.build();

        //load points
        filePath = "dft-accident-data/Accidents0515.csv";
        List<AIdedPoint> codedPoints = AccidentDataLoader.loadCsvFile(filePath);
        System.out.println("Points: " + codedPoints.size());
        
        ConcurrentLinkedQueue<Point> pointQueue = new ConcurrentLinkedQueue<>();
        long time = System.currentTimeMillis();
        codedPoints.stream().parallel().forEach(point -> {
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
                pointQueue.add(point.getPoint());
            } 
        });
        System.out.println("Covered points: " + pointQueue.size());
        System.out.println("Elapsed time: " + ((System.currentTimeMillis() - time)) / 60000d + " minutes");
    }
}
