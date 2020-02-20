package com.westar.api.practice;

import com.westar.api.datatypes.ConnectedCarEvent;
import com.westar.api.practice.function.ConnectedCarAssigner;
import com.westar.api.practice.function.CreateStopSegment;
import com.westar.api.practice.function.StopSegmentEvictor;
import com.westar.api.practice.function.StopSegmentOutOfOrderTrigger;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * 实时计算每辆车的 StopSegment
 */
public class DrivingSegment {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

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
                .window(GlobalWindows.create())//先将分组之后的数据放在全局的窗口
                .trigger(new StopSegmentOutOfOrderTrigger())// 当 speed == 0 的事件来的时候就触发 window 的计算
                .evictor(new StopSegmentEvictor())
                .process(new CreateStopSegment())
                .print();


        env.execute("DrivingSegment");

    }

}
