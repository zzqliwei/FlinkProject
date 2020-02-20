package com.westar.api.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * EventTime 处理的是时间按照事件本身的时间
 */
public class WindowWordCount {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String ip;
        try {
            ip = parameterTool.get("ip");
        } catch (Exception e) {
            System.err.println("需要提供 --ip 参数");
            return;
        }

        int port;
        try {
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            System.err.println("需要提供 --port 参数");
            return;
        }


        //1、初始化流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 本地启动的时候，默认的并行度等于你的电脑的核心数
        // Flink 程序是有一个默认的最大并行度，默认值是 128
        // 可以设置最大并行度
        env.setMaxParallelism(12);
        // 设置每个 operator 的并行度（全局范围）
        env.setParallelism(2);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. Data Source
        // 从 socket 中读取数据
        System.out.println("ip = " + ip + " and port = " + port);
        // socket 数据源不是一个可以并行的数据源
        DataStreamSource<String> dataStreamSource = env.socketTextStream(ip,port);
        // 3. Data Process
        // 3.1 对每一行按照空格切割，得到所有单词，并且可以对每个单词先计数 1
        // non keyed stream
        DataStream<Tuple2<String,Integer>> wordOnes = dataStreamSource.flatMap(new WordOneFlatMapFunction());

        // non keyed window ：不能设置并行度的
        // 每隔 3 秒钟计算所有单词的个数,属于global window 默认不会触发计算
        AllWindowedStream<Tuple2<String,Integer>, TimeWindow> nonKeyWindow = wordOnes.timeWindowAll(Time.seconds(5L));

        // 3.2 按照单词进行分组, 聚合计算每个单词出现的次数
        // keyed stream
        KeyedStream<Tuple2<String,Integer>, Tuple> wordGroup = wordOnes.keyBy(0);
        // keyed window ；可以设置并行度
        // 每隔 3 秒钟计算每个单词出现的次数 GlobalWindow
        WindowedStream<Tuple2<String,Integer>,Tuple, GlobalWindow> keyedWindow = wordGroup
                // global window 默认不会触发计算
                // global window + trigger
                //1、写的方法
//                .window(GlobalWindows.create())
//                .window(SlidingProcessingTimeWindows.of(Time.hours(1), Time.minutes(30), Time.minutes(15)))
//                .trigger(CountTrigger.of(3));
                //等价于
                .countWindow(3);

        // TimeWindow
//        WindowedStream<Tuple2<String,Integer>,Tuple, TimeWindow> keydWindowTime = wordGroup
//        .window(SlidingProcessingTimeWindows.of(Time.hours(1), Time.minutes(30), Time.minutes(15)));
        // Tumbling Window : 窗口的大小是固定的
        // 每个小时的 15 分钟开始，到下一个小时的 15 分钟结束
//        .window(TumblingProcessingTimeWindows.of(Time.hours(1), Time.minutes(15)))
//        .timeWindow(Time.seconds(3));

        DataStream<Tuple2<String, Integer>> wordCounts = keyedWindow.sum(1);

        // 4. Data Sink
        wordCounts.print().setParallelism(1);

        env.execute("WindowWordCount");
    }

    private static class WordOneFlatMapFunction implements FlatMapFunction<String, Tuple2<String,Integer>> {

        @Override
        public void flatMap(String line, Collector<org.apache.flink.api.java.tuple.Tuple2<String, Integer>> out) throws Exception {
            String[] words = line.split(" ");
            for(String word: words){
                org.apache.flink.api.java.tuple.Tuple2<String,Integer> tuple2 = new org.apache.flink.api.java.tuple.Tuple2<>(word,1);
                out.collect(tuple2);
            }
        }
    }
}
