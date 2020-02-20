package com.westar.api.practice.function;

import com.westar.api.datatypes.ConnectedCarEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.PriorityQueue;

/**
 *  思路：
 *  当每次接收到一个元素的时候，先将这个事件放到 PriorityQueue，按照 event time 进行升序排列
 *  当乱序的事件都到了，则触发定时器将排序好的事件输出
 */
public class EventSortFunction extends KeyedProcessFunction<String, ConnectedCarEvent, ConnectedCarEvent> {
    public static final OutputTag<String> outputTag = new OutputTag<String>("late_data"){};

    // 使用 PriorityQueue 对同一个 car 的所有事件进行排序
    private ValueState<PriorityQueue<ConnectedCarEvent>> queueValueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 注册状态
        ValueStateDescriptor<PriorityQueue<ConnectedCarEvent>> descriptor = new ValueStateDescriptor<PriorityQueue<ConnectedCarEvent>>(
                "sorted-events",
                TypeInformation.of(new TypeHint<PriorityQueue<ConnectedCarEvent>>() {})
        );
        queueValueState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(ConnectedCarEvent event, Context ctx, Collector<ConnectedCarEvent> out) throws Exception {
        // 拿到当前事件的 event time
        long currentEventTime = ctx.timestamp();
        // 拿到当前的 watermark 值
        TimerService timerService =ctx.timerService();
        //被调节过的时间
        long currentWatermark = timerService.currentWatermark();

        // event 属于正常的数据
        if(currentEventTime > currentWatermark){
            PriorityQueue<ConnectedCarEvent> queue = queueValueState.value();
            if (queue == null) {
                queue = new PriorityQueue<>();
            }
            // 将事件放到优先级队列中
            queue.add(event);
            queueValueState.update(queue);

            // 当当前的 watermark 值到了当前的 event 的 event time 的时候才触发定时器
            // 相当于每一个 event 等待了 30 秒钟再输出
            timerService.registerEventTimeTimer(event.getTimestamp());
        }else{
            //属于迟到太多的 数据
            // 收集 id
            ctx.output(outputTag, event.getId());
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<ConnectedCarEvent> out) throws Exception {
        // 用于输出排好序的事件
        PriorityQueue<ConnectedCarEvent> queue = queueValueState.value();
        ConnectedCarEvent head = queue.peek();
        // 拿到当前的 watermark 值
        TimerService timerService =ctx.timerService();
        //被调节过的时间
        long currentWatermark = timerService.currentWatermark();
        // 输出的条件：
        // 1. 队列不能为空
        // 2. 拿出来的事件的 event time 需要小于当前的 watermark
        while (null != head && head.getTimestamp() <= currentWatermark){
            out.collect(head);
            queue.remove(head);
            //重新获取值
            head = queue.peek();
        }

    }
}
