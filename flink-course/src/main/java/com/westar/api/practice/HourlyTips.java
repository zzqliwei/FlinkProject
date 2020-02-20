package com.westar.api.practice;

import com.westar.api.DataFilePath;
import com.westar.api.datatypes.TaxiFare;
import com.westar.api.source.GZIPFileSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 需求：实时计算每隔一个小时赚钱最多的司机
 * 1. 计算出每个小时每个司机总共赚了多少钱
 * 2. 计算出赚钱最多的司机
 */
public class HourlyTips implements DataFilePath {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 实时接收出租车收费事件
        DataStream<Tuple2<Long,Float>> driverIdWithFare = env
                .addSource(new GZIPFileSource(DataFilePath.TAXI_FARE_PATH))
                .map(new MapFunction<String, Tuple2<Long,Float>>() {
                    @Override
                    public Tuple2<Long, Float> map(String line) throws Exception {
                        TaxiFare taxiFare = TaxiFare.fromString(line);

                        return Tuple2.of(taxiFare.getDriverId(),taxiFare.getTip());
                    }
                });

        // 1. 计算出每个小时每个司机总共赚了多少钱
        // keyed window
        // 会有若干个 window ，多少个并行度，那么就有多少个 window
        DataStream<Tuple2<Long,Float>> hourlyTips = driverIdWithFare
                .keyBy(driverFare -> driverFare.f0)
                .timeWindow(Time.hours(1L))
                // 使用增量计算来解决这个性能问题：
                // 1. 当 window 中每次接收到一个元素的时候，就进行预计算
                // 2. 当触发 window 计算的时候，再对预聚合的结果进行一次聚合
                .reduce(new ReduceFunction<Tuple2<Long, Float>>() {
                    @Override
                    public Tuple2<Long, Float> reduce(Tuple2<Long, Float> t1, Tuple2<Long, Float> t2) throws Exception {
                        return Tuple2.of(t1.f0, t1.f1 + t2.f1);
                    }
                }, new ProcessWindowFunction<Tuple2<Long, Float>, Tuple2<Long, Float>, Long, TimeWindow>() {
                    @Override
                    public void process(Long key, Context context, Iterable<Tuple2<Long, Float>> elements, Collector<Tuple2<Long, Float>> out) throws Exception {
                        // 拿到每个司机赚的钱，对于每个司机，这里只会有一条数据
                        float sumTips = elements.iterator().next().f1;
                        out.collect(Tuple2.of(key,sumTips));
                    }
                });

        // 全量的计算 -> 当这个 window 数据量特别大的时候 -> 性能问题
        //.process(new AddTips());

        // 计算出赚钱最多的司机，这个时候需要将上面的所有的 window 合并成一个 window
        // non keyed window
        DataStream<Tuple2<Long,Float>> hourlyMax = hourlyTips
                .timeWindowAll(Time.hours(1))
                .maxBy(1);
        hourlyMax.print();



        env.execute("HourlyTips");
    }
}
