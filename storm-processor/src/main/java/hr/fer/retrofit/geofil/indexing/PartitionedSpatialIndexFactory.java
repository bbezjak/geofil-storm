/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hr.fer.retrofit.geofil.indexing;

import hr.fer.retrofit.geofil.indexing.SpatialIndexFactory.IndexType;
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.SpatialIndex;
import org.locationtech.jts.index.strtree.STRtree;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author Krešimir Pripužić <kresimir.pripuzic@fer.hr>
 */
public class PartitionedSpatialIndexFactory {

    public static ArrayList<SpatialIndex> create(IndexType indexType, List<Geometry> subscriptions, SpatialPartitioner partitioner) {
        ArrayList<SpatialIndex> indexes = new ArrayList<>(partitioner.numPartitions());
        for (int i = 0; i < partitioner.numPartitions(); i++) {
            indexes.add(SpatialIndexFactory.create(indexType));
        }

        //add subscriptions to partition indexes
        for (Geometry subscription : subscriptions) {
            Iterator<Tuple2<Integer, Geometry>> iterator;
            try {
                iterator = partitioner.placeObject(subscription);
                iterator.forEachRemaining(pair -> indexes.get(pair._1).insert(subscription.getEnvelopeInternal(), subscription));
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        //build spatial index in the case of STRtree
        for (SpatialIndex index : indexes) {
            if (index instanceof STRtree) {
                ((STRtree) index).build();
            }
        }

        return indexes;
    }
}
