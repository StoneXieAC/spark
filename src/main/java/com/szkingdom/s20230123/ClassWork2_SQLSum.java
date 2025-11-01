package com.szkingdom.s20230123;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.functions;

import java.util.Properties;

public class ClassWork2_SQLSum {
    public static void main(String[] args) {

        int showRows = 20;

        SparkConf conf = new SparkConf().setAppName("ClassWork2_SQLSum").setMaster("local[*]");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        String dbUrl = "jdbc:mysql://localhost:3306/spark_demo?serverTimezone=Asia/Shanghai&useSSL=false";

        Properties connectionProps = new Properties();
        connectionProps.put("user", "root");
        connectionProps.put("password", "12345");
        connectionProps.put("driver", "com.mysql.cj.jdbc.Driver");

        String csvFile = "src/main/resources/t_etl2_jjjyqrxx.csv";
        Dataset<Row> investmentsDF = spark.read()
                .option("header", "true")
                .option("sep", ",")
                .option("encoding", "GBK")
                .option("inferSchema", "true")
                .csv(csvFile);


        Dataset<Row> dailyInvestorCount = investmentsDF
                .groupBy("QRRQ", "CPDM")
                .agg(functions.countDistinct("JJZH").alias("investor_count"))
                .orderBy("QRRQ", "CPDM");

        System.out.println("========== 每日每种产品的投资者数量 ==========");
        dailyInvestorCount.show(showRows);

        dailyInvestorCount.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dbUrl, "t_fund_stat_daily", connectionProps);

        System.out.println("已写入数据库表 spark_demo.t_fund_stat_daily");

        Dataset<Row> fairAssetDF = spark.read().jdbc(dbUrl, "fairfundasset", connectionProps);

        Dataset<Row> dailyAssetSum = fairAssetDF
                .groupBy("LASTDATE")
                .agg(
                        functions.sum("FUNDTOTALVALUE").alias("total_fund_value"),
                        functions.countDistinct("HANDERNO").alias("investor_count")
                )
                .orderBy("LASTDATE");

        System.out.println("\n========== 每日投资者资产总额统计 ==========");
        dailyAssetSum.show(showRows);

        dailyAssetSum.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dbUrl, "t_asset_stat_daily", connectionProps);

        System.out.println("已写入数据库表 spark_demo.t_asset_stat_daily");

        spark.close();
    }
}
