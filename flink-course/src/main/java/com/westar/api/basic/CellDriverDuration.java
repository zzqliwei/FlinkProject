package com.westar.api.basic;

import com.westar.api.DataFilePath;
import com.westar.api.basic.function.EnrichTaxiRideFunction;
import com.westar.api.datatypes.EnrichedTaxiRide;
import com.westar.api.source.GZIPFileSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.joda.time.Interval;

/**
 * 需求：实时计算出从每一个单元格启动的车开了最长的时间。 maxBy
 */
public class CellDriverDuration implements DataFilePath {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //数据源
        DataStreamSource<String> dataStreamSource = env.addSource(new GZIPFileSource(DataFilePath.TAXI_RIDE_PATH));

        // 2. 数据处理
        // 2.1 line -> EnrichedTaxiRide
        DataStream<EnrichedTaxiRide> rides = dataStreamSource.flatMap(new EnrichTaxiRideFunction());
        // 2.2 计算出 END 事件的起始位置所属的网格以及对应的车开了多长时间
        // 即算出 <startCell, duration> ：从某一个单元格启动的车开了多长时间
        // 在每一个网格中启动的出租车到停止的时间
        DataStream<Tuple2<Integer,Integer>> minutesByStartCell = rides.flatMap(new FlatMapFunction<EnrichedTaxiRide, Tuple2<Integer, Integer>>() {
            @Override
            public void flatMap(EnrichedTaxiRide ride, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                if(!ride.isStart()){
                    // 车启动的经纬度所属的单元格
                    int startCell = ride.getStartCell();
                    Interval rideInterval = new Interval(ride.getStartTime(),ride.getEndTime());
                    // 车开了多长的时间
                    Integer duration = rideInterval.toDuration().toStandardMinutes().getMinutes();
                    out.collect(Tuple2.of(startCell,duration));
                }
            }
        });

        // 2.3 实时计算从每一个单元格启动的车开了最长的时间
        // 按照 startCell 分组，然后对 duration 聚合计算求最大值
        minutesByStartCell.keyBy(0).maxBy(1).print();

        env.execute("CellDriverDuration");
    }
}
