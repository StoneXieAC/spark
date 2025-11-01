package com.szkingdom.s20230123;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

public class ClassWork1_NEUWordsCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("ClassWork1_NEUWordsCount")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        PropertyConfigurator.configure("log4j-defaults.properties");

        String inputPath = "src/main/resources/东北大学.txt";
        JavaRDD<String> lines = sc.textFile(inputPath);

        // 单词(仅字母)出现的次数
        Pattern wordPattern = Pattern.compile("[a-zA-Z]+");
        JavaRDD<String> words = lines
                .flatMap(line -> Arrays.asList(line.split("[^a-zA-Z0-9]+")).iterator())
                .filter(word -> wordPattern.matcher(word).matches())
                .map(String::toLowerCase);

        JavaPairRDD<String, Integer> wordCounts = words
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum);

        System.out.println("========== 单词出现次数 ==========");
        wordCounts.collect().forEach(t -> System.out.println(t._1 + " : " + t._2));

        // 出现次数 >=4 次的单词
        System.out.println("\n========== 出现次数>=4的单词 ==========");
        JavaPairRDD<String, Integer> frequentWords = wordCounts.filter(t -> t._2 >= 4);
        frequentWords.collect().forEach(t -> System.out.println(t._1 + " : " + t._2));

        // 单词按字典序排序
        System.out.println("\n========== 单词按字典序排序 ==========");
        List<Tuple2<String, Integer>> sortedWords = wordCounts.sortByKey(true).collect();
        sortedWords.forEach(t -> System.out.println(t._1 + " : " + t._2));

        // 出现次数最多的数字
        System.out.println("\n========== 出现次数最多的数字 ==========");
        Pattern numberPattern = Pattern.compile("\\d+");
        JavaRDD<String> numbers = lines
                .flatMap(line -> Arrays.asList(line.split("[^0-9]+")).iterator())
                .filter(num -> numberPattern.matcher(num).matches());

        JavaPairRDD<String, Integer> numberCounts = numbers
                .mapToPair(num -> new Tuple2<>(num, 1))
                .reduceByKey(Integer::sum);

        Tuple2<String, Integer> mostCommonNumber = null;
        if (!numberCounts.isEmpty()) {
            mostCommonNumber = numberCounts.reduce((a, b) -> a._2 >= b._2 ? a : b);
        }

        if (mostCommonNumber != null) {
            System.out.println("出现次数最多的数字是：" + mostCommonNumber._1 +
                    "，出现次数：" + mostCommonNumber._2);
        } else {
            System.out.println("文本中没有数字。");
        }

        // 对所有数字求和
        System.out.println("\n========== 数字求和 ==========");
        long sum = 0L;
        if (!numbers.isEmpty()) {
            sum = numbers.map(Long::parseLong).reduce(Long::sum);
        }
        System.out.println("数字总和：" + sum);

        // 频率最高的前N个单词
        System.out.println("\n========== 前N个最常见的单词 ==========");
        Scanner scanner = new Scanner(System.in);
        System.out.print("请输入N的值：");
        int N = scanner.nextInt();

        List<Tuple2<String, Integer>> topNWords = wordCounts
                .mapToPair(t -> new Tuple2<>(t._2, t._1))
                .sortByKey(false)
                .mapToPair(t -> new Tuple2<>(t._2, t._1))
                .take(N);

        System.out.println("前 " + N + " 个单词：");
        topNWords.forEach(t -> System.out.println(t._1 + " : " + t._2));

        sc.close();
    }
}
