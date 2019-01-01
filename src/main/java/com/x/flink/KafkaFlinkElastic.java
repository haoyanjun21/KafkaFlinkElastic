package com.x.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;
import java.util.stream.IntStream;

import static com.x.flink.config.UserConfig.*;
import static com.x.flink.util.Assigner.fixedLatenessAssigner;
import static com.x.flink.util.Constant.SUM_INDEX;
import static java.lang.Double.parseDouble;

public class KafkaFlinkElastic {

    private static final int INTERVAL = 5000;
    private static final Time TIME_WINDOW = Time.seconds(5);

    public static void main(String[] args) throws Exception {
        System.setProperty("log.file", "/tmp/flink.log");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String[]> stream = addSource(env);
        for (int i = 0; i < groupIndices.size(); i++) {
            int[] groupFields = groupIndices.get(i);
            String groupIndex = String.valueOf(i);
            stream.keyBy(groupFields).timeWindow(TIME_WINDOW)
                    .reduce((ReduceFunction<String[]>) (value1, value2) -> {
                        value1[SUM_INDEX] = String.valueOf(parseDouble(value1[SUM_INDEX]) + parseDouble(value2[SUM_INDEX]));
                        return value1;
                    }).name("keyByTimeWindowReduce")
                    .map((MapFunction<String[], String[]>) values -> {
                        String[] results = new String[groupFields.length + 2];
                        IntStream.range(0, groupFields.length).forEach(j -> results[j] = values[groupFields[j]]);
                        results[groupFields.length] = values[SUM_INDEX];
                        results[results.length - 1] = groupIndex;
                        return results;
                    }).addSink(new EsSink());
            System.out.println("start " + esIndexNames.get(i));
        }
        env.execute();
    }

    private static DataStream<String[]> addSource(StreamExecutionEnvironment env) {
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(INTERVAL);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KAFKA_SERVERS);
        properties.setProperty("group.id", KAFKA_TOPIC);

        FlinkKafkaConsumer<String[]> source = new FlinkKafkaConsumer<>(KAFKA_TOPIC, new CommonSchema(), properties);
        source.setStartFromLatest();
        source.assignTimestampsAndWatermarks(fixedLatenessAssigner());
        return env.addSource(source);
    }

}
