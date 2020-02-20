package com.westar.api.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.util.Collector;

/**
 * 程序运行时间窗口
 */
public class KeyedProcessFunctionWordCount {
    public static void main(String[] args) throws Exception {
        //1、初始化流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 本地启动的时候，默认的并行度等于你的电脑的核心数
        // Flink 程序是有一个默认的最大并行度，默认值是 128
        // 可以设置最大并行度
        env.setMaxParallelism(12);
        // 设置每个 operator 的并行度（全局范围）
        env.setParallelism(2);
        //2、DataSource
        //从socet中读取数据，Socket数据源不是一个可以并行的数据源
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost",5001);
        //3、dataProcess
        //数据分割
        DataStream<Tuple2<String,Integer>> wordOnes = dataStreamSource.flatMap(new WordOneFlatMapFunction());
        // 3.2 按照单词进行分组, 聚合计算每个单词出现的次数
        // keyed stream

        // keyed stream
        KeyedStream<Tuple2<String, Integer>, Tuple> wordGroup = wordOnes
                .keyBy(0);

        DataStream<Tuple2<String,Integer>> wordCounts = wordGroup
                //1、 session window 的 gap 是静态的
//                 .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5L)))
                //2、 session window 的 DynamicGap 是动态的
                .window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Tuple2<String,Integer>>() {
                    @Override
                    public long extract(Tuple2<String, Integer> element) {
                        //返回的是时间窗口
                        if (element.f0.equals("this")) {
                            //10s
                            return 10000;
                        } else if (element.f0.equals("is")) {
                            //20s
                            return  20000;
                        }
                        //5s
                        return 5000;
                    }
                }))
                .sum(1);

                //3、运行程序
//              .process(new CountWithTimeoutFunction());

        // 4. Data Sink
        wordCounts.print().setParallelism(1);
        // 5. 启动并执行流程序
        env.execute("KeyedProcessFunctionWordCount");
    }

    private static class CountWithTimestamp {
        public String key;
        public int count;
        public long lastModified;
    }

    private static class WordOneFlatMapFunction implements FlatMapFunction<String,Tuple2<String,Integer>>{

        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = line.split(" ");
            for(String word: words){
                Tuple2<String,Integer> tuple2 = new Tuple2<>(word,1);
                out.collect(tuple2);
            }
        }
    }

    /**
     * 程序运行时间窗口
     */
    private static class CountWithTimeoutFunction extends KeyedProcessFunction<Tuple,Tuple2<String,Integer>,Tuple2<String,Integer>>{

        private ValueState<CountWithTimestamp> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<CountWithTimestamp> descriptor = new ValueStateDescriptor<CountWithTimestamp>(
                    "myState", CountWithTimestamp.class);
            valueState = getRuntimeContext().getState(descriptor);
        }

        /**
         *  处理每一个接收到的单词(元素)
         * @param element   输入元素
         * @param ctx   上下文
         * @param out   用于输出
         * @throws Exception
         */
        @Override
        public void processElement(Tuple2<String, Integer> element, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 拿到当前 key 的对应的状态
            CountWithTimestamp currentState = valueState.value();
            if(null == currentState){
                currentState = new CountWithTimestamp();
                currentState.key = element.f0;
            }
            // 更新这个 key 出现的次数
            currentState.count ++;
            // 更新这个 key 到达的时间，最后修改这个状态时间为当前的 Processing Time
            currentState.lastModified= ctx.timerService().currentProcessingTime();

            // 更新状态
            valueState.update(currentState);

            // 注册一个定时器
            // 注册一个以 Processing Time 为准的定时器
            // 定时器触发的时间是当前 key 的最后修改时间加上 5 秒
            ctx.timerService().registerProcessingTimeTimer(currentState.lastModified + 5000);
        }

        /**
         *  定时器需要运行的逻辑
         * @param timestamp 定时器触发的时间戳
         * @param ctx   上下文
         * @param out   用于输出
         * @throws Exception
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 先拿到当前 key 的状态
            CountWithTimestamp curr = valueState.value();
            // 检查这个 key 是不是 5 秒钟没有接收到数据
            if(timestamp == curr.lastModified + 5000){
                out.collect(Tuple2.of(curr.key,curr.count));
                valueState.clear();
            }
        }
    }
}
