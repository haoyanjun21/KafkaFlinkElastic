package com.x.flink.util;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.x.flink.util.Constant.SEPARATOR;

public class Util {
    public static String getString(int[] conf) {
        return IntStream.range(1, conf.length).mapToObj(i -> SEPARATOR + conf[i]).collect(Collectors.joining("", String.valueOf(conf[0]), ""));
    }
}
