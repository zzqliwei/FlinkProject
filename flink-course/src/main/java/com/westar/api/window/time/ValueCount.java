package com.westar.api.window.time;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

/**
 * EventTime 计算
 */
public class ValueCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setMaxParallelism(12);
        env.setParallelism(2);
        // 默认的情况， Flink 使用 Processing Time
        // 设置使用 Event Time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 默认的话 watermark 产生的周期是 200 ms
        System.out.println(env.getConfig().getAutoWatermarkInterval());
        // 设置 watermark 产生的周期为 1000ms
        env.getConfig().setAutoWatermarkInterval(1000);

        DataStreamSource<String> dataStreamSource = env.addSource(new TestSource());
        dataStreamSource.map(new MapFunction<String, Tuple2<String,Long>>() {
            @Override
            public Tuple2<String, Long> map(String line) throws Exception {
                String[] word = line.split(",");

                return Tuple2.of(word[0],Long.parseLong(word[1]));
            }
        })
        // 设置获取 Event Time 的逻辑
        .assignTimestampsAndWatermarks(new EventTimeExtractor())
                .keyBy(0)
                .timeWindow(Time.seconds(10),Time.seconds(5))
                .process(new ValueCountProcessWindowFunction())
                .print();

        env.execute("ValueCount");
    }

    private static class EventTimeExtractor implements AssignerWithPeriodicWatermarks<Tuple2<String, Long>>{
        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            //在这个会一直执行
//            System.out.println("generate watermark at : " + System.currentTimeMillis());
            // 延迟 5 秒触发计算 window
            return new Watermark(System.currentTimeMillis() - 5000);
        }

        @Override
        public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
            System.out.println("current element is : " + element);
            System.out.println("current event time is : " + dateFormat.format(element.f1));
            return element.f1;
        }
    }

    private static class ValueCountProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, Tuple, TimeWindow> {

        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

        @Override
        public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Tuple2<String, Long>> out) throws Exception {
            //当前时间
            System.out.println("当前时间：" + dateFormat.format(System.currentTimeMillis()));
            //处理时间
            System.out.println("处理时间："+dateFormat.format(context.currentProcessingTime()));
            //窗口开始时间
            System.out.println("窗口开始时间:"+dateFormat.format(context.window().getStart()));
            //数据量大小
            long count = Iterables.size(elements);
            out.collect(Tuple2.of(tuple.getField(0), count));
            //窗口结束时间
            System.out.println("窗口结束时间:"+dateFormat.format(context.window().getEnd()));

        }
    }
}
