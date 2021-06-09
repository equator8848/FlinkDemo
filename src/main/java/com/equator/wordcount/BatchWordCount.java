package com.equator.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

/**
 * 批处理WC
 *
 * @Author: Equator
 * @Date: 2021/4/21 9:51
 **/

public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 从文件中读取数据
        String inputPath = "src/main/resources/wordcount.txt";
        DataSet<String> inputDataSet = env.readTextFile(inputPath);
        // 数据处理，按照空格分词展开，转换为(word,num)二元组进行统计
        // groupBy 0 按第一个位置的数据分组
        // sum 1 按照第二个位置的数据求和
        DataSet<Tuple2<String, Integer>> outputDataSet = inputDataSet.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
            // 分词
            String[] words = value.split(" ");
            // 遍历所有word，包装为二元组
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }).returns((TypeInformation) TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class))
                .groupBy(0).sum(1);
        outputDataSet.print();
    }
}
