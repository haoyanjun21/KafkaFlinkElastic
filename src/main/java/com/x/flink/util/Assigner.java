package com.x.flink.util;

import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import static com.x.flink.util.Constant.TIME_INDEX;
import static java.lang.Long.parseLong;

public class Assigner {

    public static AscendingTimestampExtractor<String[]> ascendingAssigner() {
        return new AscendingTimestampExtractor<String[]>() {
            @Override
            public long extractAscendingTimestamp(String[] values) {
                return parseLong(values[TIME_INDEX]);
            }
        };
    }

    public static BoundedOutOfOrdernessTimestampExtractor<String[]> fixedLatenessAssigner() {
        return new BoundedOutOfOrdernessTimestampExtractor<String[]>(Time.seconds(10)) {
            @Override
            public long extractTimestamp(String[] values) {
                return parseLong(values[TIME_INDEX]);
            }
        };
    }
}
