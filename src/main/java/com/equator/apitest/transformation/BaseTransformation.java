package com.equator.apitest.transformation;

import com.equator.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * 基本的数据转换
 *
 * @Author: Equator
 * @Date: 2021/6/19 19:14
 **/

public class BaseTransformation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 加上这句代码可以实现顺序输出
        env.setParallelism(1);

        DataStreamSource<String> fileDataStreamSource = env.readTextFile("src/main/resources/data/sensordata.txt");

        DataStream<Integer> mapStream = fileDataStreamSource.map(String::length);


        DataStream<String> flatMapStream = fileDataStreamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                String[] fields = line.split(",");
                for (String field : fields) {
                    collector.collect(field);
                }
            }
        });

        SingleOutputStreamOperator<String> filterStream = fileDataStreamSource.filter((line) -> line.startsWith("sensor_1"));

        mapStream.print("map");
        flatMapStream.print("flatMap");
        filterStream.print("filter");

        env.execute();
    }
}
