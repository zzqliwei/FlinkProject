package com.westar.api.basic;

import com.westar.api.DataFilePath;
import com.westar.api.basic.function.EnrichTaxiRideFunction;
import com.westar.api.basic.function.NYCFilter;
import com.westar.api.datatypes.EnrichedTaxiRide;
import com.westar.api.datatypes.TaxiRide;
import com.westar.api.source.GZIPFileSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *  需求：计算每一个 TaxiRide 的起始位置所在单元格以及结束位置所在单元格，并打印
 */
public class RideEnrichingWithFlatMap implements DataFilePath {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1. 数据源
        DataStreamSource<String> dataStreamSource = env.addSource(new GZIPFileSource(DataFilePath.TAXI_RIDE_PATH));

        // 2. 数据处理
        // 2.1 将每一行字符串转成 TaxiRide 类型
        DataStream<EnrichedTaxiRide> enrichedTaxiRides = dataStreamSource
                .flatMap(new EnrichTaxiRideFunction() );
        enrichedTaxiRides.print();

        env.execute("RideEnrichingWithFlatMap");
    }
}
