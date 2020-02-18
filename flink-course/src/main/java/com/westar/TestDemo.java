package com.westar;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestDemo {
    public static void main(String[] args) {
        List<String> lines = new ArrayList<>();
        lines.add("this is an example");
        lines.add("for someone");
        lines.add("this is awesome");

        // Stream 中的 flatMap
        // 输入是一个，输出可能是若干个(0,1,2,3.....)
        // 一 对 多
        lines.stream().flatMap(s ->{
            String[] words = s.split(" ");
            return Arrays.stream(words);
        }).forEach((word) -> System.out.println(word));

    }
}
