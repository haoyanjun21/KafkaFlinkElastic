package com.x.flink.util;

import static com.x.flink.util.Constant.SEPARATOR;

public class Util {
    public static String getString(int[] conf) {
        StringBuilder sb = new StringBuilder(String.valueOf(conf[0]));
        for (int i = 1; i < conf.length; i++) {
            sb.append(SEPARATOR).append(conf[i]);
        }
        return sb.toString();
    }
}
