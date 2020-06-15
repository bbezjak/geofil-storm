/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hr.fer.retrofit.geofil.loading;

import hr.fer.retrofit.geofil.wrapping.CodedMultiPolygon;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.store.ContentFeatureCollection;
import org.geotools.data.store.ContentFeatureSource;
import org.locationtech.jts.geom.MultiPolygon;
import org.opengis.feature.simple.SimpleFeature;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 *
 * @author Krešimir Pripužić <kresimir.pripuzic@fer.hr>
 */
public class PostcodeDataLoader {
    public static List<CodedMultiPolygon> loadShapeFile(String filePath) throws MalformedURLException, NoSuchElementException, IOException {
        //sectors
        List<CodedMultiPolygon> result = new LinkedList<>();
        File sectorsFile = new File(filePath);
        ShapefileDataStore dataStore = new ShapefileDataStore(sectorsFile.toURI().toURL());
        ContentFeatureSource featureSource = dataStore.getFeatureSource();
        ContentFeatureCollection featureCollection = featureSource.getFeatures();
        try ( SimpleFeatureIterator iterator = featureCollection.features()) {
            while (iterator.hasNext()) {
                SimpleFeature feature = iterator.next();
                String code = (String) feature.getAttribute(2);
                MultiPolygon multyPolygon = (MultiPolygon) feature.getDefaultGeometry();
                //if (code.startsWith("RG")) {
                result.add(new CodedMultiPolygon(multyPolygon, code));
                //}                
            }
        }
        dataStore.dispose();
        return result;
    }
}
