package com.x.flink.util;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import static com.x.flink.config.UserConfig.KAFKA_BROKERS;
import static com.x.flink.config.UserConfig.KAFKA_TOPIC;
import static com.x.flink.util.Constant.CHARSET_NAME;
import static com.x.flink.util.Constant.DELIMITER;

public class KafkaProduce extends Thread {
    private final KafkaProducer<String, String> producer;

    private KafkaProduce() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KAFKA_BROKERS);
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(properties);
    }

    public static void main(String[] args) {
        System.out.println("KafkaProduce start ...");
        int threadCount = 2;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            executor.submit(new KafkaProduce());
        }
    }

    private void sendLogMessage(String... param) {
        try {
            String st = String.join(DELIMITER, param);
            System.out.println(DELIMITER + st + DELIMITER);
            producer.send(new ProducerRecord<>(KAFKA_TOPIC, new String(st.getBytes(), CHARSET_NAME)));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        String[] client = {"android", "apple", "m"};
        String[] network = {"2g", "3g", "4g", "wifi"};
        Random random = new Random();
        int i = 0;
        while (++i <= 10000) {
            sendLogMessage(
                    String.valueOf(System.currentTimeMillis()),
                    random.nextInt(5) > 1 ? "1" : "",
                    random.nextInt(5) > 1 ? "cn" : "id",
                    random.nextInt(255) + "." + random.nextInt(255) + "." + random.nextInt(255) + "." + random.nextInt(255),
                    "functionId" + random.nextInt(3),
                    client[random.nextInt(3)],
                    random.nextInt(3) + ".0.0",
                    "uuid" + random.nextInt(100),
                    "pin" + random.nextInt(100),
                    "partner" + random.nextInt(3),
                    "osVersion" + random.nextInt(3),
                    network[random.nextInt(4)],
                    random.nextInt(5) * 100 + ""
            );
            try {
                sleep(ThreadLocalRandom.current().nextInt(10));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("循环结束,发送了 " + --i + " 条");
    }
}