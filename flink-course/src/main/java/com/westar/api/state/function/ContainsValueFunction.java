package com.westar.api.state.function;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class ContainsValueFunction extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, String>> {

    private AggregatingState<Long,String> totalStr;

    @Override
    public void open(Configuration parameters) throws Exception {
        AggregatingStateDescriptor<Long,String,String> descriptor = new AggregatingStateDescriptor<Long, String, String>(
                "totalStr",//状态的名字
                new AggregateFunction<Long, String, String>() {
                    @Override
                    public String createAccumulator() {
                        return "Contains：";
                    }

                    @Override
                    public String add(Long value, String accumulator) {
                        if("Contains：".equals(accumulator)){
                            return accumulator + value;
                        }else{
                            return accumulator +" and "+ value;
                        }
                    }

                    @Override
                    public String getResult(String accumulator) {
                        return accumulator;
                    }

                    @Override
                    public String merge(String a, String b) {
                        return a +" and " + b;
                    }
                }, String.class);
        totalStr = getRuntimeContext().getAggregatingState(descriptor);
    }
    @Override
    public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, String>> out) throws Exception {
        totalStr.add(element.f1);
        out.collect(Tuple2.of(element.f0,totalStr.get()));
    }
}
