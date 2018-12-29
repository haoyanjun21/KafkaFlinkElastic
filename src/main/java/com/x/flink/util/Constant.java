package com.x.flink.util;

import static com.x.flink.config.UserConfig.USER_FIELDS;

public class Constant {
    public static final String CHARSET_NAME = "utf-8";
    public static final String DELIMITER = "\n";
    public static final String CONNECTOR = "_";
    public static final String ES_TYPE = "x";
    public static final String SEPARATOR = ",";
    public static final String STATISTICAL_FIELD = "sum";
    /**
     * 在用户配置的字段后 加入 统计字段
     */
    public static final String[] FIELDS = (USER_FIELDS + SEPARATOR + STATISTICAL_FIELD).toLowerCase().split(SEPARATOR);
    /**
     * 统计字段的 索引位置
     */
    public static final int SUM_INDEX = FIELDS.length - 1;
    /**
     * 时间戳的 索引位置
     */
    public static final int TIME_INDEX = 0;
    /**
     * 全链路压测字段标识的 索引位置
     */
    public static final int FORCE_BOT_INDEX = 1;
}
