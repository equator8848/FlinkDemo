package com.equator.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 流处理
 *
 * @Author: Equator
 * @Date: 2021/4/21 15:33
 **/

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从文件中读取数据
        String inputPath = "src/main/resources/wordcount.txt";
        DataStream<String> inputDataStream = env.readTextFile(inputPath);

        // 基于数据流进行转换计算
        // FlatMapFunction 在通用方法中，写法一样
        DataStream<Tuple2<String, Integer>> outputStream = inputDataStream.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
            // 分词
            String[] words = value.split(" ");
            // 遍历所有word，包装为二元组
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }).returns((TypeInformation) TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class))
                .keyBy(0)
                .sum(1);
        outputStream.print();
        // 执行任务，Flink流计算基于事件触发
        env.execute();
        /**
         *          2> (hello,1)
         *          2> (c,1)
         *          2> (hello,2)
         *          2> (hello,3)
         *          2> (javascript,1)
         *          2> (hello,4)
         *          2> (hello,5)
         *          1> (java,1)
         *          1> (redis,1)
         *          2> (hello,6)
         *          2> (hello,7)
         *          2> (python,1)
         *          4> (cpp,1)
         *          2> (hello,8)
         *          2> (hello,9)
         *          4> (mysql,1)
         *          3> (spring,1)
         *          3> (world,1)
         *          前面的数字表示并行执行的线程分区，默认并行度为4
         */
    }
}
