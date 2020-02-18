package com.westar;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * socketTextStream
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String ip;
        try {
            ip = parameterTool.get("ip");
        }catch (Exception e){
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

        //1、初始化流的环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(5001);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //本地启动的时候，默认的并行度等于电脑的核心数
        //Flink 有一个默认的并行度，就是128
        //可以设置一个最大的并行度
        env.setMaxParallelism(12);
        // 设置每个 operator 的并行度（全局范围）
        env.setParallelism(2);

        //2、Data Source
        // 从 socket 中读取数据
        System.out.println("ip = " + ip + " and port = " + port);
        DataStreamSource<String> dataStreamSource = env.socketTextStream(ip, port);
        // 3.1 对每一行按照空格切割，得到所有单词，并且可以对每个单词先计数 1
        DataStream<Tuple2<String,Integer>> wordOnes = dataStreamSource.flatMap(new WordOneFlatMapFunction());
        // 3.2 按照单词进行分组, 聚合计算每个单词出现的次数
        DataStream<Tuple2<String,Integer>> wordCounts = wordOnes
                .keyBy(0)
                .sum(1);
        // 4. Data Sink
        wordCounts.print().setParallelism(1);
        //5、启动执行程序
        env.execute("WordCount");

    }
    private static class WordOneFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>>{
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = line.toLowerCase().split(" ");
            for(String word:words){
                Tuple2<String,Integer> wordOne = new Tuple2<>(word,1);
                out.collect(wordOne);
            }
        }
    }
}
