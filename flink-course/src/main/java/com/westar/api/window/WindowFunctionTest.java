package com.westar.api.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowFunctionTest {
    public static void main(String[] args) throws Exception {
        // 1. 初始化一个流执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        // 本地启动的时候，默认的并行度等于你的电脑的核心数
        // Flink 程序是有一个默认的最大并行度，默认值是 128
        // 可以设置最大并行度
        env.setMaxParallelism(12);
        // 设置每个 operator 的并行度（全局范围）
        env.setParallelism(2);

        // 2. Data Source
        // 从 socket 中读取数据
        // socket 数据源不是一个可以并行的数据源
        DataStreamSource<String> dataStreamSource =
                env.socketTextStream("localhost", 5001);
        // 3. Data Process
        // 3.1 对每一行按照空格切割，得到所有单词，并且可以对每个单词先计数 1
        // non keyed stream
        DataStream<Tuple2<String,Integer>> users = dataStreamSource.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String line) throws Exception {
                String[] strings = line.split(",");
                return Tuple2.of(strings[0], Integer.valueOf(strings[1]));
            }

        });

        // 3.2 按照单词进行分组, 聚合计算每个单词出现的次数
        // keyed stream
        KeyedStream<Tuple2<String, Integer>, Tuple> userIdGroup = users
                .keyBy(0);
        // keyed window ；可以设置并行度
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> keyedWindow = userIdGroup
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        DataStream<Tuple2<String, Integer>> result = keyedWindow.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> a, Tuple2<String, Integer> b) throws Exception {
                return Tuple2.of(a.f0,a.f1 + b.f1);
            }
        });
        //求平均值
        keyedWindow.aggregate(new AverageAggregate());

        keyedWindow.process(new MyProcessWindowFunction()).print();

        // 4. Data Sink
        // result.print().setParallelism(1);


        env.execute("WindowFunctionTest");
    }

    private static class MyProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Integer>, String, Tuple, TimeWindow> {

        /**
         *  处理一个 window 中的所有元素
         *  这里是按照单独的 key 来定义的计算逻辑
         * @param tuple key
         * @param context   上下文
         * @param elements  当前 key 在当前的 window 中的所有元素
         * @param out   用于输出
         * @throws Exception
         */
        @Override
        public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {
            // 统计相同的 key 出现的次数
            long count = 0L;
            for(Tuple2<String, Integer> ele:elements){
                count ++;
            }
            // 输出
            out.collect("Window ：" + context.window() + ", key :" + tuple + ", count ：" + count);
        }
    }

    private static class AverageAggregate implements AggregateFunction<Tuple2<String, Integer>, Tuple2<Integer, Integer>, Double> {

        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            return Tuple2.of(0, 0);
        }

        @Override
        public Tuple2<Integer, Integer> add(Tuple2<String, Integer> element, Tuple2<Integer, Integer> accumulator) {
            return Tuple2.of(accumulator.f0 + 1,element.f1 + accumulator.f1);
        }

        @Override
        public Double getResult(Tuple2<Integer, Integer> acc) {
            return (double)acc.f1 / acc.f0;
        }

        @Override
        public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> acc, Tuple2<Integer, Integer> acc1) {
            return Tuple2.of(acc.f0 + acc1.f0,acc.f1 + acc1.f1);
        }
    }
}
