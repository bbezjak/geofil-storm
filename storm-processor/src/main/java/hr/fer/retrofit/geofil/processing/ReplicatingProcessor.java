/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hr.fer.retrofit.geofil.processing;

import hr.fer.retrofit.geofil.indexing.SpatialIndexFactory;
import hr.fer.retrofit.geofil.indexing.SpatialIndexFactory.IndexType;
import org.geotools.geojson.geom.GeometryJSON;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.SpatialIndex;
import org.locationtech.jts.index.quadtree.Quadtree;
import org.locationtech.jts.index.strtree.STRtree;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Stream;

import static hr.fer.retrofit.geofil.processing.ReplicatingProcessor.ThrowingFunction.unchecked;

/**
 *
 * @author Krešimir Pripužić <kresimir.pripuzic@fer.hr>
 */
public class ReplicatingProcessor {

    public static void main(String[] args) throws IOException {
        //load subscriptions from disk
        Stream<String> lines = Files.lines(Paths.get("subscriptions19.json"));

        SpatialIndex index = SpatialIndexFactory.create(IndexType.QUAD_TREE);
        GeometryJSON gj = new GeometryJSON(19);

        //parse and add subscriptions to index
        lines.map(unchecked(line -> gj.read(line))).forEach(geometry
                -> index.insert(geometry.getEnvelopeInternal(), geometry));

        //build spatial index in the case of STRtree
        if (index instanceof STRtree) {
            ((STRtree) index).build();
        }

        //check if all subscriptions are indexed
        if (index instanceof STRtree) {
            System.out.println("Number of indexed subscriptions: " + ((STRtree) index).size());
        }       
        if (index instanceof Quadtree) {
            System.out.println("Number of indexed subscriptions: " + ((Quadtree) index).size());
        } 

        ConcurrentMap matchingPairs = new ConcurrentHashMap();

        //load publications from disk and process them
        lines = Files.lines(Paths.get("publications19.json"));
        lines.parallel().map(unchecked(line -> gj.read(line))).forEach(publication -> {
            List<Geometry> matchingSubscriptions = index.query(publication.getEnvelopeInternal());

            for (Geometry matchingSubscription : matchingSubscriptions) {
                if (matchingSubscription.covers(publication)) {
                    matchingPairs.put(matchingSubscription, publication);
                }
            }
        });

        System.out.println("Number of pairs: " + matchingPairs.size());
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
