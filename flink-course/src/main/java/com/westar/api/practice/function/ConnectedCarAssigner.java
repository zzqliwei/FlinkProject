package com.westar.api.practice.function;

import com.westar.api.datatypes.ConnectedCarEvent;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
/**
 *  watermark 产生机制：
 *  1. 每次接收到一个事件的话就会生成一个 watermark
 */
public class ConnectedCarAssigner implements AssignerWithPunctuatedWatermarks<ConnectedCarEvent> {
    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(ConnectedCarEvent lastElement, long extractedTimestamp) {
        // 设置 watermark 值为当前事件的 event time 减去 30 秒
        // 需要注意，watermark 在以下两种情况下是不会变化的：
        // 1. 返回 null
        // 2. 当前返回的 watermark 值比上一次返回的 watermark 值还要小的时候
        return new Watermark(extractedTimestamp - 30000);
    }

    @Override
    public long extractTimestamp(ConnectedCarEvent element, long previousElementTimestamp) {
        return element.getTimestamp();
    }
}
