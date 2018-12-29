package com.x.flink;

import com.x.flink.config.UserConfig;
import com.x.flink.util.Constant;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

import static com.x.flink.util.Constant.SUM_INDEX;
import static java.lang.Double.parseDouble;
import static java.lang.Long.parseLong;

public class KafkaFlinkElastic {

    private static final int INTERVAL = 5000;
    private static final Time TIME_WINDOW = Time.milliseconds(1000);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String[]> stream = addSource(env);
        for (int i = 0; i < UserConfig.groupIndices.size(); i++) {
            int[] group = UserConfig.groupIndices.get(i);
            String groupIndex = String.valueOf(i);
            stream.keyBy(group).timeWindow(TIME_WINDOW)
                    .reduce((ReduceFunction<String[]>) (value1, value2) -> {
                        double sum = parseDouble(value1[SUM_INDEX]) + parseDouble(value2[SUM_INDEX]);
                        value1[SUM_INDEX] = String.valueOf(sum);
                        return value1;
                    }).name("keyByTimeWindowReduce")
                    .map((MapFunction<String[], String[]>) values -> {
                        String[] results = new String[group.length + 2];
                        for (int j = 0; j < group.length; j++) {
                            results[j] = values[group[j]];
                        }
                        results[group.length] = values[SUM_INDEX];
                        results[group.length + 1] = groupIndex;
                        System.out.println(Arrays.asList(results));
                        return results;
                    }).addSink(new EsSink());
        }
        env.execute();
    }

    private static DataStream<String[]> addSource(StreamExecutionEnvironment env) {
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(INTERVAL);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", UserConfig.KAFKA_BROKERS);
        properties.setProperty("group.id", UserConfig.KAFKA_TOPIC);

        FlinkKafkaConsumer<String[]> source = new FlinkKafkaConsumer<>(UserConfig.KAFKA_TOPIC, new CommonSchema(), properties);
        source.setStartFromLatest();
        source.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String[]>() {
            private static final long serialVersionUID = 4187525445835099672L;

            @Override
            public long extractAscendingTimestamp(String[] values) {
                return parseLong(values[Constant.TIME_INDEX]);
            }
        });
        return env.addSource(source);
    }

}
