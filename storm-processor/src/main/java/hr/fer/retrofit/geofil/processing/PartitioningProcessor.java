/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hr.fer.retrofit.geofil.processing;

import hr.fer.retrofit.geofil.indexing.PartitionedSpatialIndexFactory;
import hr.fer.retrofit.geofil.indexing.SpatialIndexFactory.IndexType;
import hr.fer.retrofit.geofil.partitioning.SpatialPartitionerFactory;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner;
import org.geotools.geojson.geom.GeometryJSON;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.SpatialIndex;
import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Stream;

import static hr.fer.retrofit.geofil.processing.PartitioningProcessor.ThrowingFunction.unchecked;

/**
 *
 * @author Krešimir Pripužić <kresimir.pripuzic@fer.hr>
 */
public class PartitioningProcessor {

    private static final int NUMBER_OF_PARTITIONS = 20;
    private static final GridType GRID_TYPE = GridType.VORONOI;

    public static void main(String[] args) throws IOException, Exception {
        //load subscriptions from disk
        Stream<String> lines = Files.lines(Paths.get("subscriptions19.json"));

        List<Geometry> subscriptions = new LinkedList<>();
        GeometryJSON gj = new GeometryJSON(19);

        //parse and add subscriptions to list
        lines.map(unchecked(line -> gj.read(line))).forEach(geometry
                -> subscriptions.add(geometry));
        System.out.println("Number of added subscriptions: " + subscriptions.size());

        //create list of subscription envelopes
        List<Envelope> subscriptionEnvelopes = new LinkedList<>();
        for (Geometry subscription : subscriptions) {
            subscriptionEnvelopes.add(subscription.getEnvelopeInternal());
        }

        //create a partitioner using subscriptions envelopes
        SpatialPartitioner partitioner = SpatialPartitionerFactory.create(GRID_TYPE, NUMBER_OF_PARTITIONS, subscriptionEnvelopes);

        //create a partitioned index (i.e. a spatial index for each partition)
        ArrayList<SpatialIndex> partitionedIndex = PartitionedSpatialIndexFactory.create(IndexType.STR_TREE, subscriptions, partitioner);
        

        ConcurrentMap matchingPairs = new ConcurrentHashMap();

        //load publications from disk and process them
        lines = Files.lines(Paths.get("publications19.json"));
        lines.parallel().map(unchecked(line -> gj.read(line))).forEach(publication -> {
            try {
                Iterator<Tuple2<Integer, Geometry>> iterator = partitioner.placeObject(publication);
                iterator.forEachRemaining(pair -> {
                    SpatialIndex subscriptionIndex = partitionedIndex.get(pair._1);

                    List<Geometry> matchingSubscriptions = subscriptionIndex.query(publication.getEnvelopeInternal());

                    for (Geometry matchingSubscription : matchingSubscriptions) {
                        if (matchingSubscription.covers(publication)) {
                            matchingPairs.put(matchingSubscription, publication);
                        }
                    }
                });
            } catch (Exception ex) {
                ex.printStackTrace();
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
