package com.equator.apitest.transformation;

import com.equator.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 滚动聚合
 *
 * @Author: Equator
 * @Date: 2021/6/19 22:26
 **/

public class RollingAggregationTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 加上这句代码可以实现顺序输出
        env.setParallelism(1);

        DataStreamSource<String> fileDataStreamSource = env.readTextFile("src/main/resources/data/sensordata.txt");

        // String数据包装为SensorReading
        DataStream<SensorReading> dataStream = fileDataStreamSource.map((line) -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");
        DataStream<SensorReading> maxStream = keyedStream.max("temperature");

        maxStream.print();

        env.execute();
    }
}
