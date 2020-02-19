package com.westar.api.basic.function;

import com.westar.api.datatypes.EnrichedTaxiRide;
import com.westar.api.datatypes.TaxiRide;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class EnrichTaxiRideFunction implements FlatMapFunction<String, EnrichedTaxiRide> {
    @Override
    public void flatMap(String line, Collector<EnrichedTaxiRide> out) throws Exception {

        TaxiRide taxiRide = TaxiRide.fromString(line);

        if(new NYCFilter().filter(taxiRide)){
            EnrichedTaxiRide enrichedTaxiRide = new EnrichedTaxiRide(taxiRide);
            out.collect(enrichedTaxiRide);
        }


    }
}
