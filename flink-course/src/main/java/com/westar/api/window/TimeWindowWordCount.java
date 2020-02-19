package com.westar.api.window;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 *  每隔 5 秒钟统计前 10 秒内每个单词出现的次数
 */
public class TimeWindowWordCount {
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

        // 1. 初始化一个流执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        // 本地启动的时候，默认的并行度等于你的电脑的核心数
        // Flink 程序是有一个默认的最大并行度，默认值是 128
        // 可以设置最大并行度
        env.setMaxParallelism(12);
        // 设置每个 operator 的并行度（全局范围）
        env.setParallelism(2);

        // 2. Data Source
        // 从 socket 中读取数据
        System.out.println("ip = " + ip + " and port = " + port);
        // socket 数据源不是一个可以并行的数据源
        DataStreamSource<String> dataStreamSource = env.socketTextStream(ip, port);
        // 3. Data Process
        // 3.1 对每一行按照空格切割，得到所有单词，并且可以对每个单词先计数 1
        DataStream<Tuple2<String,Integer>> wordOnes = dataStreamSource
                .flatMap(new WordOneFlatMapFunction());

        // 3.2 按照单词进行分组, 聚合计算每个单词出现的次数
        DataStream<Tuple2<String,Integer>> wordCount = wordOnes
                .keyBy(0)
                // 每隔 5 秒钟统计前 10 秒内每个单词出现的次数
        .timeWindow(Time.seconds(10), Time.seconds(5))
                .process(new SumProcessWindowFunction());

        env.execute("TimeWindowWordCount");

    }

    private static class SumProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Integer>,
            Tuple2<String, Integer>, Tuple, TimeWindow> {

        private FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");



        /**
         *  当 window 触发计算的时候会调用这个方法
         *  处理一个 window 中单个 key 对应的所有的元素
         *
         *  window 中的每一个 key 会调用一次
         *
         * @param tuple  key
         * @param context   operator 的上下文
         * @param elements  指定 window 中指定 key 对应的所有的元素
         * @param out   用于输出
         * @throws Exception
         */
        @Override
        public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> elements,
                            Collector<Tuple2<String, Integer>> out) throws Exception {
            System.out.println("-----------------------------------------------------");
            System.out.println("当前系统的时间：" + dateFormat.format(System.currentTimeMillis()));
            System.out.println("window 处理时间：" + dateFormat.format(context.currentProcessingTime()));
            System.out.println("window 的开始时间：" + dateFormat.format(context.window().getStart()));
            // 每个单词在指定的 window 中出现的次数
            int sum = 0;
            for (Tuple2<String, Integer> ele : elements) {
                sum += 1;
            }
            // 输出单词出现的次数
            out.collect(Tuple2.of(tuple.getField(0), sum));
            System.out.println("window 的结束时间：" + dateFormat.format(context.window().getEnd()));

        }
    }

    private static class WordOneFlatMapFunction implements FlatMapFunction<String, Tuple2<String,Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String,Integer>> out) throws Exception {
            String[] words = line.split(" ");
            for(String word: words){
                out.collect(new Tuple2<String,Integer>(word,1));
            }

        }
    }
}
