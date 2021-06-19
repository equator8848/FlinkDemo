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

public class SourceBaseTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 加上这句代码可以实现顺序输出
        env.setParallelism(1);

        DataStream<SensorReading> dataStreamSource = env.fromCollection(Arrays.asList(
                new SensorReading("sensor1", 1547718201L, 7.7),
                new SensorReading("sensor2", 1547718206L, 16.5),
                new SensorReading("sensor3", 1547718212L, 13.2)
        ));
        dataStreamSource.print("collection");

        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9);
        integerDataStreamSource.print("element");


        // 从文件中读取，需要自己进行转换与包装
        DataStreamSource<String> fileDataStreamSource = env.readTextFile("src/main/resources/data/sensordata.txt");
        fileDataStreamSource.print("file");

        env.execute();
    }
}
