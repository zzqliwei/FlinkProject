package com.westar;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * fromElements
 */
public class WordCountSource {
    public static void main(String[] args) throws Exception {
        //1、初始化流的环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //本地启动的时候，默认的并行度等于电脑的核心数
        //Flink 有一个默认的并行度，就是128
        //可以设置一个最大的并行度
        env.setMaxParallelism(12);
        // 设置每个 operator 的并行度（全局范围）
        env.setParallelism(2);

        // 2. Data Source

        DataStreamSource<String> dataStreamSource = env
                .fromElements("this is an example", "this is the first example");

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
    private static class WordOneFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {
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
