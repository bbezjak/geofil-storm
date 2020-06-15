/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hr.fer.retrofit.geofil.partitioning;

import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.spatialPartitioning.*;
import org.datasyslab.geospark.spatialPartitioning.quadtree.QuadTreePartitioner;
import org.locationtech.jts.geom.Envelope;

import java.util.List;

/**
 *
 * @author Krešimir Pripužić <kresimir.pripuzic@fer.hr>
 */
public class SpatialPartitionerFactory {

    public static SpatialPartitioner create(GridType gridType, int numPartitions, List<Envelope> subscriptionEnvelopes)
            throws Exception {
        if (numPartitions <= 0) {
            throw new IllegalArgumentException("Number of partitions must be >= 0");
        }

        Envelope boundaryEnvelope = new Envelope();
        for (Envelope envelope : subscriptionEnvelopes) {
            boundaryEnvelope.expandToInclude(envelope);

        }

        final Envelope paddedBoundary = new Envelope(
                boundaryEnvelope.getMinX(), boundaryEnvelope.getMaxX() + 0.01,
                boundaryEnvelope.getMinY(), boundaryEnvelope.getMaxY() + 0.01);

        switch (gridType) {
            case EQUALGRID: {
                EqualPartitioning EqualPartitioning = new EqualPartitioning(paddedBoundary, numPartitions);
                return new FlatGridPartitioner(EqualPartitioning.getGrids());
            }
            case HILBERT: {
                HilbertPartitioning hilbertPartitioning = new HilbertPartitioning(subscriptionEnvelopes, paddedBoundary, numPartitions);
                return new FlatGridPartitioner(hilbertPartitioning.getGrids());
            }
            case RTREE: {
                RtreePartitioning rtreePartitioning = new RtreePartitioning(subscriptionEnvelopes, numPartitions);
                return new FlatGridPartitioner(rtreePartitioning.getGrids());
            }
            case VORONOI: {
                VoronoiPartitioning voronoiPartitioning = new VoronoiPartitioning(subscriptionEnvelopes, numPartitions);
                return new FlatGridPartitioner(voronoiPartitioning.getGrids());
            }
            case QUADTREE: {
                QuadtreePartitioning quadtreePartitioning = new QuadtreePartitioning(subscriptionEnvelopes, paddedBoundary, numPartitions);
                return new QuadTreePartitioner(quadtreePartitioning.getPartitionTree());
            }
            case KDBTREE: {
                final KDBTree tree = new KDBTree(subscriptionEnvelopes.size() / numPartitions, numPartitions, paddedBoundary);
                for (final Envelope sample : subscriptionEnvelopes) {
                    tree.insert(sample);
                }
                tree.assignLeafIds();
                return new KDBTreePartitioner(tree);
            }
            default: return null;
        }
    }
}
