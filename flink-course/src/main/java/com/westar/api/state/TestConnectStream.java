package com.westar.api.state;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * 无序的join
 */
public class TestConnectStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //数据源
        DataStream<String> control = env.fromElements("DROP", "IGNORE").keyBy(x -> x);

        DataStream<String> streamOfWords =
                env.fromElements("data", "DROP", "artisans", "IGNORE").keyBy(x -> x);

        // 输出：
        // artisans
        // data
        // 将控制流中的数据放到某个状态中
        // 数据输入流从状态中查询单词是否存在，存在的话，则不输出
        control.connect(streamOfWords)
                .flatMap(new ControlFunction())
                .print();
        env.execute("TestConnectStream");
    }

    private static class ControlFunction extends RichCoFlatMapFunction<String, String, String>{
        private ValueState<Boolean> blocked;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<Boolean>(
                    "block",Boolean.class );
            blocked = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void flatMap1(String value, Collector<String> out) throws Exception {
            // 接收到控制流数据的时候会触发的计算
            // 如果接收到控制流中的数据的时候，直接设置状态为 true
            System.out.println(value);
            blocked.update(Boolean.TRUE);
        }

        @Override
        public void flatMap2(String value, Collector<String> out) throws Exception {
            // 接收到数据输入流的时候会触发的计算
            // 如果当前的 key (单词) 不在状态中的话，也就是在控制流中不存在
            // 输出数据
            if(blocked.value() == null || !blocked.value()){
                out.collect(value);
            }
        }
    }
}
