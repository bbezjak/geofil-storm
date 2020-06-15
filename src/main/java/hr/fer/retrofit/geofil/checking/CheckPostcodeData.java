package hr.fer.retrofit.geofil.checking;

import hr.fer.retrofit.geofil.wrapping.CodedMultiPolygon;
import org.locationtech.jts.index.strtree.STRtree;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static hr.fer.retrofit.geofil.loading.PostcodeDataLoader.loadShapeFile;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Krešimir Pripužić <kresimir.pripuzic@fer.hr>
 */
public class CheckPostcodeData {

    public static void main(String[] args) throws MalformedURLException, IOException {

        //load areas
        String filePath = "GB_Postcodes-QGIS_fixed/PostalArea_fixed.shp";
        List<CodedMultiPolygon> areas = loadShapeFile(filePath);
        System.out.println("Areas: " + areas.size());

        //load districts
        filePath = "GB_Postcodes-QGIS_fixed/PostalDistrict_fixed.shp";
        List<CodedMultiPolygon> districts = loadShapeFile(filePath);
        System.out.println("Districts: " + districts.size());

        //count districts covered by area
        STRtree districtIndex = new STRtree();
        for (CodedMultiPolygon district : districts) {
            districtIndex.insert(district.getMultiPolygon().getEnvelopeInternal(), district);
        }
        districtIndex.build();

        Set<CodedMultiPolygon> districtSet = ConcurrentHashMap.newKeySet();
        areas.stream().parallel().forEach(area -> {
            //System.out.print(area.getCode() + ": ");
            List<CodedMultiPolygon> matchingDistricts = districtIndex.query(area.getMultiPolygon().getEnvelopeInternal());
            for (CodedMultiPolygon matchingDistrict : matchingDistricts) {
                if (area.getMultiPolygon().covers(matchingDistrict.getMultiPolygon())) {
                    districtSet.add(matchingDistrict);
                }
            }
            //System.out.println("");
        });
        System.out.println("Covered districts: " + districtSet.size());

        //find non-covered districts
        STRtree areaIndex = new STRtree();
        for (CodedMultiPolygon area : areas) {
            areaIndex.insert(area.getMultiPolygon().getEnvelopeInternal(), area);
        }
        areaIndex.build();
        
        districts.stream().parallel().forEach(district -> {
            List<CodedMultiPolygon> matchingAreas = areaIndex.query(district.getMultiPolygon().getEnvelopeInternal());
            boolean flag = false;

            for (CodedMultiPolygon matchingArea : matchingAreas) {
                if (matchingArea.getMultiPolygon().covers(district.getMultiPolygon())) {
                    flag = true;
                } else {
                    matchingArea.getMultiPolygon().covers(district.getMultiPolygon());
                }
            }

            if (!flag) {
                System.out.println(district.getCode());
            }
        });
        
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

        Set<CodedMultiPolygon> sectorSet = ConcurrentHashMap.newKeySet();
        districts.stream().parallel().forEach(district -> {
            //System.out.print(district.getCode() + ": ");
            List<CodedMultiPolygon> matchingSectors = sectorIndex.query(district.getMultiPolygon().getEnvelopeInternal());
            for (CodedMultiPolygon matchingSector : matchingSectors) {
                if (district.getMultiPolygon().covers(matchingSector.getMultiPolygon())) {
                    //System.out.print(matchingSector.getCode() + ", ");
                    sectorSet.add(matchingSector);
                }
            }
            //System.out.println("");
        });
        System.out.println("Covered sectors: " + sectorSet.size());

        //find non-covered sectors
        sectors.stream().parallel().forEach(sector -> {
            List<CodedMultiPolygon> matchingDistricts = districtIndex.query(sector.getMultiPolygon().getEnvelopeInternal());
            boolean flag = false;

            for (CodedMultiPolygon matchingDistrict : matchingDistricts) {
                if (matchingDistrict.getMultiPolygon().covers(sector.getMultiPolygon())) {
                    flag = true;
                } else {
                    matchingDistrict.getMultiPolygon().covers(sector.getMultiPolygon());
                }
            }

            if (!flag) {
                System.out.println(sector.getCode());
            }
        });
    }    
}
