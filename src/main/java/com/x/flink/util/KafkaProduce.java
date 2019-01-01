package com.x.flink.util;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import static com.x.flink.config.UserConfig.KAFKA_SERVERS;
import static com.x.flink.config.UserConfig.KAFKA_TOPIC;
import static com.x.flink.util.Constant.CHARSET_NAME;
import static com.x.flink.util.Constant.DELIMITER;

public class KafkaProduce extends Thread {
    private final KafkaProducer<String, String> producer;

    private KafkaProduce() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KAFKA_SERVERS);
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(properties);
    }

    public static void main(String[] args) {
        System.out.println("KafkaProduce start ...");
        int threadCount = 1;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        IntStream.range(0, threadCount).mapToObj(i -> new KafkaProduce()).forEach(executor::submit);
    }

    private void sendLogMessage(String... param) {
        try {
            String st = String.join(DELIMITER, param);
            producer.send(new ProducerRecord<>(KAFKA_TOPIC, new String(st.getBytes(), CHARSET_NAME)));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        String[] client = {"android", "apple", "m"};
        String[] network = {"3g", "4g", "wifi"};
        Random random = new Random();
        int i = 0;
        while (++i <= 100000000) {
            int bound = 2;
            int ip = random.nextInt(bound);
            sendLogMessage(
                    String.valueOf(System.currentTimeMillis()),
                    random.nextInt(bound) > 1 ? "1" : "",
                    random.nextInt(bound) > 1 ? "cn" : "th",
                    ip + "." + ip + "." + ip + "." + ip,
                    "functionId" + random.nextInt(bound),
                    client[random.nextInt(bound) % 3],
                    random.nextInt(bound) + ".0.0",
                    "uuid" + random.nextInt(bound),
                    "pin" + random.nextInt(bound),
                    "partner" + random.nextInt(bound),
                    "osVersion" + random.nextInt(bound),
                    network[random.nextInt(bound) % 3],
                    String.valueOf(random.nextInt(bound))
            );
            try {
                if (i % 1000 == 0)
                    Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("循环结束,发送了 " + --i + " 条" + System.currentTimeMillis());
    }
}