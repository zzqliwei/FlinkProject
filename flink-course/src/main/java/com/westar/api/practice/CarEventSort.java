package com.westar.api.practice;

import com.westar.api.datatypes.ConnectedCarEvent;
import com.westar.api.practice.function.ConnectedCarAssigner;
import com.westar.api.practice.function.EventSortFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *  实时对无序的 Car Event 中的每一辆车所有的事件按照时间升序排列
 *  1. 需要读取数据源，并将字符串转成 ConnectedCarEvent
 *  2. 按照 carId 分组，然后对每个 carId 所有的事件按照 event time 升序排列
 */
public class CarEventSort {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //读取数据源
        DataStreamSource<String> carData = env.readTextFile("flink-course\\data\\car\\carOutOfOrder.csv");
        //字符串转成ConnectedCarEvent
        DataStream<ConnectedCarEvent> events = carData
                .map((String line) -> ConnectedCarEvent.fromString(line))
                .assignTimestampsAndWatermarks(new ConnectedCarAssigner());
        // 对每辆车的事件按照 event time 排序
        SingleOutputStreamOperator<ConnectedCarEvent> sortedEvents = events
                .keyBy(ConnectedCarEvent::getCarId)
                .process(new EventSortFunction());

        sortedEvents.getSideOutput(EventSortFunction.outputTag).print();


        env.execute("CarEventSort");
    }
}
