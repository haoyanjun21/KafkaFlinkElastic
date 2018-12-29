# Kafka Flink Elastic

安装依赖的组件

    elasticsearch 
    kibana 
    kafka   

创建kafka测试主题

    /usr/local/Cellar/kafka/2.1.0/bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic topic_xxx

运行 KafkaProduce （向kafka生产测试数据）

运行 KafkaFlinkElastic （消费kafka数据，按 UserConfig 中的配置进行分组计算，计算结果存入ES）

使用 kibana 查看计算结果



