package com.x.flink.config;

import com.x.flink.util.Util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.x.flink.util.Constant.*;

public class UserConfig {
    //用户配置的 kafka、es
    public static final String KAFKA_SERVERS = "localhost:9092";
    //用户配置的 应用名
    private static final String APP_NAME = "xxx";
    public static final String KAFKA_TOPIC = "topic_" + APP_NAME;
    public static final String ES_HOSTNAME = "localhost";
    public static final int ES_PORT = 9200;
    public static final String ES_INDEX = APP_NAME + "_es_sink_statistics";
    //用户配置的 上报字段
    public static final String USER_FIELDS = "second,forceBot,type,ip,functionId,client,version,uuid,pin,partner,osVersion,network,responseTime";
    //用户配置的 计算分组
    public static String[] groupConfigs = {"second,forceBot,ip,uuid,pin", "second,forceBot,type,functionId,client,version,partner,osVersion,network"};
    //在USER_FIELDS对应的索引位置
    public static List<int[]> groupIndices = new ArrayList<>();
    public static List<String> esIndexNames = new ArrayList<>();

    //将用户配置的分组groupConfigs 转化为 在USER_FIELDS对应的索引位置
    static {
        List<String> filedList = Arrays.asList(FIELDS);
        for (int i = 0, length = groupConfigs.length; i < length; i++) {
            groupConfigs[i] = groupConfigs[i].toLowerCase();
            groupIndices.add(Arrays.stream(groupConfigs[i].split(SEPARATOR)).mapToInt(filedList::indexOf).toArray());
            esIndexNames.add(APP_NAME + CONNECTOR + groupConfigs[i].replaceAll(SEPARATOR, CONNECTOR));
        }
    }

    public static void main(String[] args) {
        System.out.println("用户配置的 上报字段");
        System.out.println(USER_FIELDS);

        System.out.println("用户配置的 计算分组");
        Arrays.stream(groupConfigs).forEach(System.out::println);

        System.out.println("生成的 计算分组的索引位置");
        groupIndices.stream().map(Util::getString).forEach(System.out::println);

        System.out.println("生成的 es 索引名");
        esIndexNames.forEach(System.out::println);
    }

}
