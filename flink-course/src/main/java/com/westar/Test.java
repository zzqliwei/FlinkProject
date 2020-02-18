package com.westar;

import org.apache.flink.util.MathUtils;

/**
 * 测试类 技术啊hash值，并更根据hash值判定 执行任务的executor和任务
 */
public class Test {
    public static void main(String[] args) {
        //128 是默认的并行度
        int keyGroupId = MathUtils.murmurHash("this".hashCode()) % 12;
        int operatorTaskIndex = keyGroupId * 4 / 12;
        int taskNumber = operatorTaskIndex + 1;
        System.out.println(taskNumber);
    }
}
