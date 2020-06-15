/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hr.fer.retrofit.geofil.wrapping;

import org.locationtech.jts.geom.Point;

/**
 *
 * @author Krešimir Pripužić <kresimir.pripuzic@fer.hr>
 */
public class AIdedPoint {
    private final Point point;
    private final String ai;

    public AIdedPoint(Point point, String ai) {
        this.point = point;
        this.ai = ai;
    }

    public Point getPoint() {
        return point;
    }

    public String getAI() {
        return ai;
    }        
}
