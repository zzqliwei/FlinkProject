package com.westar.api.datatypes;

import com.westar.api.util.GeoUtils;

import java.io.Serializable;

public class EnrichedTaxiRide extends TaxiRide {

    // 起始位置所属网格的 id
    private int startCell;
    // 终止位置所属网格的 id
    private int endCell;

    public EnrichedTaxiRide() {}

    public EnrichedTaxiRide(TaxiRide ride) {
        super.copyData(ride);
        this.startCell = GeoUtils.mapToGridCell(ride.getStartLon(), ride.getStartLat());
        this.endCell = GeoUtils.mapToGridCell(ride.getEndLon(), ride.getEndLat());
    }


    public int getStartCell() {
        return startCell;
    }

    public void setStartCell(int startCell) {
        this.startCell = startCell;
    }

    public int getEndCell() {
        return endCell;
    }

    public void setEndCell(int endCell) {
        this.endCell = endCell;
    }

    @Override
    public String toString() {
        return "EnrichedTaxiRide{" +
                "startCell=" + startCell +
                ", endCell=" + endCell + "," +
                super.toString() +
                '}';
    }

}
