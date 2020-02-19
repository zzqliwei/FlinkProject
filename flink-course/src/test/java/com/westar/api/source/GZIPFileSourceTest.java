package com.westar.api.source;

import com.westar.api.DataFilePath;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class GZIPFileSourceTest implements DataFilePath {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        GZIPFileSource source = new GZIPFileSource(DataFilePath.TAXI_RIDE_PATH);

        DataStreamSource<String> dataStreamSource = env
                .addSource(source);

        dataStreamSource.print().setParallelism(1);

        env.execute("GZIPFileSourceTest");
    }
}
