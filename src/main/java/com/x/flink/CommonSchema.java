package com.x.flink;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.UnsupportedEncodingException;

import static com.x.flink.util.Constant.*;


public class CommonSchema extends AbstractDeserializationSchema<String[]> {

    @Override
    public String[] deserialize(byte[] message) throws UnsupportedEncodingException {
        String st = new String(message, CHARSET_NAME);
        //增加统计字段值 默认值1
        String[] values = (st + DELIMITER + "1").split(DELIMITER);
        //时间戳 转秒
        values[TIME_INDEX] = String.valueOf(Long.parseLong(values[TIME_INDEX]) / 1000 * 1000);
        //全链路压测标识 转化
        values[FORCE_BOT_INDEX] = "".equals(values[1]) ? "F" : "T";
        return values;
    }

}
