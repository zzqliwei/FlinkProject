package com.westar.api.basic.function;

import com.westar.api.datatypes.TaxiRide;
import com.westar.api.util.GeoUtils;
import org.apache.flink.api.common.functions.FilterFunction;

public class NYCFilter implements FilterFunction<TaxiRide> {
    @Override
    public boolean filter(TaxiRide ride) throws Exception {
        return GeoUtils.isInNYC(ride.getStartLon(), ride.getStartLat()) &&
                GeoUtils.isInNYC(ride.getEndLon(), ride.getEndLat());
    }
}
