package com.equator.apitest.transformation;

import com.equator.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

/**
 * 滚动聚合
 *
 * @Author: Equator
 * @Date: 2021/6/19 22:26
 **/

public class ConnectUnionStreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 加上这句代码可以实现顺序输出
        env.setParallelism(1);

        DataStreamSource<String> fileDataStreamSource = env.readTextFile("src/main/resources/data/sensordata.txt");

        DataStream<SensorReading> dataStream = fileDataStreamSource.map((line) -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        SplitStream<SensorReading> splitStream = dataStream.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensorReading) {
                // 设置标签 低温与高温
                if (sensorReading.getTemperature() > 30) {
                    return Collections.singletonList("high");
                }
                return Collections.singletonList("low");
            }
        });

        DataStream<SensorReading> highStream = splitStream.select("high");
        DataStream<SensorReading> lowStream = splitStream.select("low");
        DataStream<SensorReading> allStream = splitStream.select("high", "low");

        // 将高温流转换为二元组类型，与低温流连接合并之后，输出状态信息
        DataStream<Tuple2<String, Double>> dangerousStream = highStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
                return new Tuple2<>(sensorReading.getId(), sensorReading.getTemperature());
            }
        });

        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStreams = dangerousStream.connect(lowStream);
        DataStream<Object> connectResultStream = connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> val) throws Exception {
                return new Tuple3<>(val.f0, val.f1, "high temperature");
            }

            @Override
            public Object map2(SensorReading val) throws Exception {
                return new Tuple2<>(val.getId(), "low temperature");
            }
        });
        connectResultStream.print();

        // Union合流
        highStream.union(lowStream, allStream);

        env.execute();
    }
}
