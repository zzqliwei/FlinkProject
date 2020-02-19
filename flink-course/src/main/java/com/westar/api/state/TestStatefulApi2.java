package com.westar.api.state;

import com.westar.api.state.function.ContainsValueFunction;
import com.westar.api.state.function.CountWindowAverageWithValueState;
import com.westar.api.state.function.SumFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 需求：当接收到的相同 key 的元素个数等于 3 个或者超过 3 个的时候
 *  就计算这些元素的 value 的平均值。
 *  计算 keyed stream 中每 3 个元素的 value 的平均值
 */
public class TestStatefulApi2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //数据源
        DataStreamSource<Tuple2<Long, Long>> dataStreamSource =
                env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L),
                        Tuple2.of(2L, 4L), Tuple2.of(2L, 2L), Tuple2.of(2L, 5L));

        dataStreamSource
                .keyBy(0)
//                .flatMap(new ContainsValueFunction())
                .flatMap(new SumFunction())
                .print();
        //等价于
        dataStreamSource
                .keyBy(0)
                .sum(1)
                .print();
        env.execute("TestStatefulApi2");
    }
}
