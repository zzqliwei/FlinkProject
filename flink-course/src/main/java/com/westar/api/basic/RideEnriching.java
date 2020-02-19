package com.westar.api.basic;

import com.westar.api.DataFilePath;
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
public class RideEnriching implements DataFilePath {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1. 数据源
        DataStreamSource<String> dataStreamSource = env.addSource(new GZIPFileSource(DataFilePath.TAXI_RIDE_PATH));

        // 2. 数据处理
        // 2.1 将每一行字符串转成 TaxiRide 类型
        DataStream<TaxiRide> rides = dataStreamSource
                .map(line -> TaxiRide.fromString(line));
        // 2.2 过滤出起始位置和终止位置都在纽约地理范围内的事件
        DataStream<TaxiRide> filteredRides = rides.filter(new NYCFilter());

        // 2.3. 计算每一个事件起始位置所属的网格 id 以及停止位置所属的网格 id
        DataStream<EnrichedTaxiRide> enrichedTaxiRides = filteredRides.
                map(ride->new EnrichedTaxiRide(ride));

        enrichedTaxiRides.print().setParallelism(1);
        env.execute("RideEnriching");
    }
}
