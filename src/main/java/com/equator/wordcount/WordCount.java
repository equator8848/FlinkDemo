package com.equator.wordcount;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @Author: Equator
 * @Date: 2021/6/7 8:17
 **/
@Slf4j
public class WordCount {
    public static void main(String[] args) throws Exception {
        // 设置运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置Kafka参数
        Properties conf = new Properties();
        // 外部系统 conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9092;kafka2:9093;kafka3:9094");
        conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9092;kafka2:9092;kafka3:9092");
        conf.put(ConsumerConfig.GROUP_ID_CONFIG, "flink-group");

        // 设置数据源
        String inputTopic = "flink-wordcount-topic";
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(inputTopic, new SimpleStringSchema(), conf);
        DataStream<String> stream = env.addSource(consumer);

        // 使用Flink API 对输入文本流进行操作
        DataStream<Tuple2<String, Integer>> wordCount = stream.flatMap((String line, Collector<Tuple2<String, Integer>> collector) -> {
            String[] tokens = line.split("\\s");
            // 输出结果
            for (String token : tokens) {
                if (token.length() > 0) {
                    collector.collect(new Tuple2<>(token, 1));
                }
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);
        // Sink
        wordCount.print();
        // 执行
        env.execute("Kafka Stream WordCount");
    }
}
