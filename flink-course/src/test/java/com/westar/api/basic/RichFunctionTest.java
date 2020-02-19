package com.westar.api.basic;

import com.westar.api.DataFilePath;
import com.westar.api.source.GZIPFileSource;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class RichFunctionTest implements DataFilePath {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置全局参数
        Configuration configuration = new Configuration();
        configuration.setString("testKey", "testValue");
        env.getConfig().setGlobalJobParameters(configuration);

        //1、数据源
        DataStreamSource<String> dataStreamSource = env.fromElements("this is an example", "this is my example", "this is my example");
        // 3. Data Process
        // 3.1 对每一行按照空格切割，得到所有单词，并且可以对每个单词先计数 1
        DataStream<Tuple2<String,Integer>> wordOnes = dataStreamSource
                .flatMap(new WordOneFlatMapFunction())
                .name("flatMap operator");

        // 3.2 按照单词进行分组, 聚合计算每个单词出现的次数
        DataStream<Tuple2<String,Integer>> wordCounts = wordOnes.keyBy(0).sum(1);

        // 4. Data Sink
        wordCounts.print();

        env.execute("RichFunctionTest");
    }

    private static class WordOneFlatMapFunction extends RichFlatMapFunction<String, Tuple2<String, Integer>> {

        private String value = null;

        /**
         * // 在 Operator SubTask 初始化的时候会调用一次
         *  而且仅仅就调用一次,总调用次数会超过1次
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open");
            // 可以执行一次性的任务，比如：
            // 1. 加载一次性的静态数据
            // 2. 建立和外部服务通讯的连接
            // 3. 访问配置参数 (Flink Batch API)

            // 还可以访问当前的 operator task 的运行时上下文
            RuntimeContext rc = getRuntimeContext();
            System.out.println(rc.getTaskName());

            // 获取全局参数
            ExecutionConfig.GlobalJobParameters globalJobParameters = rc.getExecutionConfig()
                    .getGlobalJobParameters();

            Configuration configuration = (Configuration) globalJobParameters;
            value = configuration.getString("testKey",null);
        }

        @Override
        public void flatMap(String line, Collector<Tuple2<String,Integer>> out) throws Exception {
            String[] words = line.split(" ");
            for(String word : words){
                out.collect(Tuple2.of(word,1));
            }
        }

        // 当 job 结束的时候，每个 Operator SubTask 关闭的时候会调用一次
        // 而且仅仅就调用一次

        @Override
        public void close() throws Exception {
            System.out.println("close");
            // 关闭资源
        }
    }
}
