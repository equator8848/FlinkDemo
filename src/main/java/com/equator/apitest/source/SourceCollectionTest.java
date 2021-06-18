package com.equator.apitest.source;

import com.equator.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @Author: Equator
 * @Date: 2021/6/18 22:00
 **/

public class SourceCollectionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.setParallelism(1); 加上这句代码可以实现顺序输出

        DataStream<SensorReading> dataStreamSource = env.fromCollection(Arrays.asList(
                new SensorReading("sensor1", 1547718201L, 7.7),
                new SensorReading("sensor2", 1547718206L, 16.5),
                new SensorReading("sensor3", 1547718212L, 13.2)
        ));

        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9);

        // 从文件中读取，需要自己进行转换与包装
        // DataStreamSource<String> stringDataStreamSource = env.readTextFile("");

        dataStreamSource.print("sensor");
        integerDataStreamSource.print("int");

        env.execute();
    }
}
