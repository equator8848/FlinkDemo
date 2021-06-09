package com.equator.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 流处理
 *
 * @Author: Equator
 * @Date: 2021/4/21 15:33
 **/

public class RealStreamWordCount {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从socket文本流中读取数据（借助netcat工具模拟数据流）
        // 可以在Linux服务器中开启NC监听服务 nc -kl 7777 （注意关闭防火墙）

        // 使用ParameterTool从程序启动参数中提取配置项
        ParameterTool pl = ParameterTool.fromArgs(args);
        // Program Argument 配置 --host 192.168.19.128 --port 7777
        String host = pl.get("host");
        int port = pl.getInt("port");
        DataStream<String> inputDataStream = env.socketTextStream(host, port);

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
    }
}
