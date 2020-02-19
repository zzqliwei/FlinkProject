package com.westar.api.basic;

import com.westar.api.DataFilePath;
import com.westar.api.datatypes.TaxiRide;
import com.westar.api.source.GZIPFileSource;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 计算每一个司机的开车次数
 */
public class RideCount implements DataFilePath {
    public static void main(String[] args) throws Exception {
        //初始化流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1. 数据源
        DataStreamSource<String> dataStreamSource = env.addSource(new GZIPFileSource(DataFilePath.TAXI_RIDE_PATH));
        // 2. 数据处理
        // 2.1 将每一行字符串转成 TaxiRide 类型
        //  2.2 过滤出是启动事件的 TaxiRide
        DataStream<TaxiRide> rides = dataStreamSource.map(line -> TaxiRide.fromString(line)).filter(taxiRide -> taxiRide.isStart());
        // 2.3 将每一个 TaxiRide 转换成 <driverId, 1> 二元组类型
        DataStream<Tuple2<Long,Long>> driverOne = rides
                .map(ride -> Tuple2.of(ride.getDriverId(),1L))
                .returns(Types.TUPLE(Types.LONG, Types.LONG));

        // 2.4 按照 driverId 进行分组
        // 2.5 聚合计算每一个司机开车的次数
        DataStream<Tuple2<Long,Long>> rideCounts =  driverOne.keyBy(0).sum(1);

        // 3. 数据打印
        rideCounts.print().setParallelism(1);

        env.execute("RideCount");


    }
}
