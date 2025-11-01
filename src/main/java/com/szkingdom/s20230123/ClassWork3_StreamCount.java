package com.szkingdom.s20230123;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

public class ClassWork3_StreamCount {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("ClassWork3_StreamCount");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(20));

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", 9999);

        // 定义只保留字母单词的正则
        Pattern wordPattern = Pattern.compile("[a-zA-Z]+");

        JavaPairDStream<String, Integer> wordCounts = lines
                .flatMap((FlatMapFunction<String, String>) line ->
                        Arrays.asList(line.split("[^a-zA-Z]+")).iterator()) // 按非字母分割
                .filter(word -> wordPattern.matcher(word).matches()) // 仅保留字母
                .map(String::toLowerCase) // 转小写
                .mapToPair((PairFunction<String, String, Integer>) word ->
                        new Tuple2<>(word, 1))
                .reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum);

        wordCounts.print();

        // 错误日志输出到 stderr
        lines
                .filter(line -> line.toLowerCase().contains("error"))
                .foreachRDD((JavaRDD<String> rdd, org.apache.spark.streaming.Time time) -> {
                    if (!rdd.isEmpty()) {
                        System.err.println("========== 错误日志批次: " + time + " ==========");
                        rdd.collect().forEach(System.err::println);
                    }
                });

        ssc.start();
        ssc.awaitTermination();
    }
}
