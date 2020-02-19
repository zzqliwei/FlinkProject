package com.westar.api.state.function;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;


/**
 *  ReducingState<T> ：这个状态为每一个 key 保存一个聚合之后的值
 *      get() 获取状态值
 *      add()  更新状态值，将数据放到状态中
 *      clear() 清除状态
 */
public class SumFunction extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
    // managed keyed state
    // 用于保存每一个 key 对应的 value 的总值
    private ReducingState<Long> sumState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ReducingStateDescriptor<Long> descriptor = new ReducingStateDescriptor<Long>(
                "sum",
                new ReduceFunction<Long>() {
                    @Override
                    public Long reduce(Long a, Long b) throws Exception {
                        return a + b;
                    }
                },Long.class);
        sumState = getRuntimeContext().getReducingState(descriptor);
    }
    @Override
    public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, Long>> out) throws Exception {
        sumState.add(element.f1);
        out.collect(Tuple2.of(element.f0,sumState.get()));
    }
}
