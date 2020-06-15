/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hr.fer.retrofit.geofil.wrapping;

import org.locationtech.jts.geom.MultiPolygon;

/**
 *
 * @author Krešimir Pripužić <kresimir.pripuzic@fer.hr>
 */
public class CodedMultiPolygon {
    private final MultiPolygon multiPolygon;
    private final String code;

    public CodedMultiPolygon(MultiPolygon multiPolygon, String code) {
        this.multiPolygon = multiPolygon;
        this.code = code;
    }

    public MultiPolygon getMultiPolygon() {
        return multiPolygon;
    }

    public String getCode() {
        return code;
    }        
}
