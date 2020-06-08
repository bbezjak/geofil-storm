/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hr.fer.retrofit.geofil.indexing;

import org.locationtech.jts.index.SpatialIndex;
import org.locationtech.jts.index.quadtree.Quadtree;
import org.locationtech.jts.index.strtree.STRtree;

/**
 *
 * @author Krešimir Pripužić <kresimir.pripuzic@fer.hr>
 */
public class SpatialIndexFactory {
    
    public enum IndexType{STR_TREE, QUAD_TREE};

    public static SpatialIndex create(IndexType indexType) {
        if (indexType == indexType.QUAD_TREE) {
            return new Quadtree();
        } else {
            return new STRtree();
        }
    }
}
