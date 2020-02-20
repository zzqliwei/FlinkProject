package com.westar.api.practice;

import com.westar.api.datatypes.ConnectedCarEvent;
import com.westar.api.datatypes.GapSegment;
import com.westar.api.practice.function.ConnectedCarAssigner;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 实时计算每辆车的 StopSegment
 */
public class DrivingSessions {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取数据源
        DataStreamSource<String> carData = env.readTextFile("flink-course\\data\\car\\carOutOfOrder.csv");
        //字符串转成ConnectedCarEvent
        DataStream<ConnectedCarEvent> events = carData
                .map(line -> ConnectedCarEvent.fromString(line))
                .assignTimestampsAndWatermarks(new ConnectedCarAssigner());
        // 按照 carId 进行分组
        events.keyBy(ConnectedCarEvent::getCarId)
                .window(EventTimeSessionWindows.withGap(Time.seconds(15)))
                .process(new ProcessWindowFunction<ConnectedCarEvent, GapSegment, String, TimeWindow>() {
                    @Override
                    public void process(String careId, Context context, Iterable<ConnectedCarEvent> elements, Collector<GapSegment> out) throws Exception {
                        GapSegment gapSegment = new GapSegment(elements);
                        out.collect(gapSegment);
                    }
                }).print();

        env.execute("DrivingSessions");
    }
}
