package com.szkingdom.s20230123;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.Properties;

import static org.apache.spark.sql.functions.*;

public class AssetCount {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("AssetCount")
                .master("local[*]")
                .getOrCreate();

        String dbAUrl = "jdbc:mysql://localhost:3306/spark_demo?serverTimezone=Asia/Shanghai&useTimezone=true";
        String dbBUrl = "jdbc:mysql://localhost:3306/fi_warehouse?serverTimezone=Asia/Shanghai&useTimezone=true";

        Properties dbProps = new Properties();
        dbProps.setProperty("user", "root");
        dbProps.setProperty("password", "12345");

        String[] tableNames = new String[]{
                "t_etl2_cp_cpgzr",
                "t_etl2_cpjbxxyxscpxx",
                "t_etl2_jjjyqrxx",
                "t_etl2_jjzhxx",
                "t_etl2_khjbxx"
        };
        for (String tableName : tableNames) {
            Dataset<Row> df = spark.read().jdbc(dbAUrl, tableName, dbProps);
            df.write().mode(SaveMode.Overwrite).jdbc(dbBUrl, tableName, dbProps);
        }

        Dataset<Row> t_jjjyqrxx = spark.read().jdbc(dbBUrl, "t_etl2_jjjyqrxx", dbProps);
        Dataset<Row> t_jjzhxx = spark.read().jdbc(dbBUrl, "t_etl2_jjzhxx", dbProps);
        Dataset<Row> t_khjbxx = spark.read().jdbc(dbBUrl, "t_etl2_khjbxx", dbProps);
        Dataset<Row> t_cp_cpgzr = spark.read().jdbc(dbBUrl, "t_etl2_cp_cpgzr", dbProps);
        Dataset<Row> t_cpjbxx = spark.read().jdbc(dbBUrl, "t_etl2_cpjbxxyxscpxx", dbProps);

        t_jjjyqrxx = t_jjjyqrxx
                .withColumn("SQRQ", to_timestamp(col("SQRQ"), "yyyyMMdd").cast(DataTypes.DateType))
                .withColumn("QRRQ", to_timestamp(col("QRRQ"), "yyyyMMdd").cast(DataTypes.DateType))
                .withColumn("SJRQ", to_timestamp(col("SJRQ"), "yyyyMMdd").cast(DataTypes.DateType))
                .withColumn("BGQJ", concat(date_format(col("SQRQ"), "yyyyMMdd"),
                        lit(" - "),
                        date_format(col("QRRQ"), "yyyyMMdd")))
                .withColumn("BGND", date_format(col("QRRQ"), "yyyy"));

        t_jjzhxx = t_jjzhxx
                .withColumn("ZHKHRQ", col("ZHKHRQ").cast(DataTypes.DateType))
                .withColumn("ZHXGRQ", col("ZHXGRQ").cast(DataTypes.DateType))
                .withColumn("SJRQ", col("SJRQ").cast(DataTypes.DateType));

        t_khjbxx = t_khjbxx.withColumn("SJRQ", to_timestamp(col("SJRQ"), "yyyyMMdd").cast(DataTypes.DateType));

        t_cpjbxx = t_cpjbxx
                .withColumn("SJRQ", to_timestamp(col("SJRQ"), "yyyyMMdd").cast(DataTypes.DateType));
        t_cp_cpgzr = t_cp_cpgzr.withColumn("SJRQ", col("SJRQ").cast(DataTypes.DateType));

        Dataset<Row> joined = t_jjjyqrxx.as("jjjyqrxx")
                .join(t_jjzhxx.as("jjzhxx"),
                        col("jjjyqrxx.JJZH").equalTo(col("jjzhxx.JJZH")), "left")
                .join(t_khjbxx.as("khjbxx"),
                        col("jjzhxx.KHBH").equalTo(col("khjbxx.KHBH")), "left")
                .join(t_cpjbxx.as("cpjbx"),
                        col("jjjyqrxx.CPDM").equalTo(col("cpjbx.zcpdm")), "left")
                .join(t_cp_cpgzr.as("cpgzr"),
                        col("jjjyqrxx.CPDM").equalTo(col("cpgzr.CPDM")), "left");

        Dataset<Row> prepared = joined.select(
                col("khjbxx.DJJGDM").alias("JGDM"),
                col("jjjyqrxx.BGND"),
                col("jjjyqrxx.BGQJ"),
                col("jjjyqrxx.SJRQ"),
                col("jjjyqrxx.CPDM"),
                col("jjjyqrxx.CNWBZ"),
                col("jjzhxx.XSSBH").alias("XSJGDM"),
                col("khjbxx.GRJGBZ").alias("ZTLB"),
                col("khjbxx.TZZLX"),
                col("khjbxx.KHBH").alias("QRYWDM"),
                col("jjjyqrxx.ZRZCBZ"),
                col("jjjyqrxx.JJZH"),
                col("jjjyqrxx.QRFE"),
                col("jjjyqrxx.QRJE"),
                col("jjjyqrxx.FYHJ"),
                col("jjjyqrxx.GHF"),
                col("jjjyqrxx.SXF"),
                col("jjjyqrxx.SXFGGLR"),
                col("jjjyqrxx.SXFGXSJG"),
                col("jjjyqrxx.SXFGCPZC"),
                col("jjjyqrxx.HSF"),
                col("jjjyqrxx.HSFGGLR"),
                col("jjjyqrxx.HSFGXSJG")
        ).na().fill("0");


        Dataset<Row> r_cisp2_a1019 = prepared.groupBy(
                        col("JGDM"),    // 机构代码
                        col("BGND"),    // 报告年度
                        col("BGQJ"),    // 报告区间
                        col("SJRQ"),    // 数据日期
                        col("CPDM"),    // 产品代码
                        col("CNWBZ"),   // 场内场外标志
                        col("XSJGDM"),  // 销售机构代码
                        col("ZTLB"),    // 主体类别
                        col("TZZLX"),   // 投资者类型
                        col("QRYWDM"),  // 确认业务代码
                        col("ZRZCBZ")   // 转入转出标志
                )
                .agg(
                        count(col("JJZH")).alias("CYJYZHSL"),   // 参与交易账户数量
                        sum(col("QRFE")).alias("QRFS"),          // 确认份数
                        sum(col("QRJE")).alias("QRJE"),          // 确认金额
                        sum(col("FYHJ")).alias("FYHJ"),          // 费用合计
                        sum(col("GHF")).alias("GHF"),            // 过户费
                        sum(col("SXF")).alias("SXF"),            // 手续费
                        sum(col("SXFGGLR")).alias("SXFGGLR"),    // 手续费（归管理人）
                        sum(col("SXFGXSJG")).alias("SXFGXSJG"),  // 手续费（归销售机构）
                        sum(col("SXFGCPZC")).alias("SXFGCPZC"),  // 手续费（归产品资产）
                        sum(col("HSF")).alias("HSF"),            // 后收费
                        sum(col("HSFGGLR")).alias("HSFGGLR"),    // 后收费（归管理人）
                        sum(col("HSFGXSJG")).alias("HSFGXSJG")   // 后收费（归销售机构）
                );

        r_cisp2_a1019 = r_cisp2_a1019.select(
                col("JGDM"), col("BGND"), col("BGQJ"),
                col("SJRQ"), col("CPDM"), col("CNWBZ"),
                col("XSJGDM"), col("ZTLB"), col("TZZLX"),
                col("QRYWDM"), col("ZRZCBZ"), col("CYJYZHSL"),
                col("QRFS"), col("QRJE"), col("FYHJ"), col("GHF"),
                col("SXF"), col("SXFGGLR"), col("SXFGXSJG"),
                col("SXFGCPZC"), col("HSF"), col("HSFGGLR"), col("HSFGXSJG")
        );

        r_cisp2_a1019.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dbBUrl, "r_cisp2_a1019", dbProps);

        System.out.println("===== 交易汇总表 r_cisp2_a1019 =====");
        r_cisp2_a1019.show(10, false);


        // 查询 1：按投资者类型与报告期汇总
        Dataset<Row> q_by_tzzlx_quarter = r_cisp2_a1019.groupBy(
                col("JGDM"), col("BGND"), col("BGQJ"), col("TZZLX")
        ).agg(
                sum(col("CYJYZHSL")).alias("acct_cnt"),
                sum(col("QRFS")).alias("conf_qty"),
                sum(col("QRJE")).alias("conf_amt"),
                sum(col("FYHJ")).alias("fee_total")
        ).orderBy(col("BGND").desc(), col("BGQJ").desc(), col("conf_amt").desc());

        System.out.println("===== 数据查询 1 =====");
        q_by_tzzlx_quarter.show(10, false);

        // 查询 2：按产品在报告期内的销售表现排行
        Dataset<Row> q_by_product = r_cisp2_a1019.groupBy(
                col("JGDM"), col("BGND"), col("BGQJ"), col("CPDM")
        ).agg(
                sum(col("CYJYZHSL")).alias("acct_cnt"),
                sum(col("QRJE")).alias("conf_amt"),
                sum(col("QRFS")).alias("conf_qty")
        ).orderBy(col("BGND").desc(), col("BGQJ").desc(), col("conf_amt").desc());

        System.out.println("===== 数据查询 2 =====");
        q_by_product.show(10, false);

        spark.stop();
    }
}