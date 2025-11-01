# 🚀 Spark 数据处理项目

## 📘 项目概述

本项目是东北大学软件学院《金融商业数据分析》课程的作业与实验项目，基于 **Apache Spark** 构建的大数据处理与分析平台。

项目使用 **Java** 语言开发，集成了 **Spark Core、Spark SQL** 与 **Spark Streaming** 技术栈，实现了从批量数据处理到实时流分析的完整流程。

项目面向金融行业的大数据应用场景，涵盖基金交易统计、资产汇总分析、文本数据处理以及实时流数据分析等功能。通过与 **MySQL 数据库** 的交互，系统实现了完整的 **ETL（提取、转换、加载）** 流程，并提供了多种统计与分析能力。

---

## 🧩 项目结构

```
src/
├── main/
│   ├── java/com/szkingdom/s20230123/
│   │   ├── AssetCount.java                # 资产统计主程序
│   │   ├── ClassWork1_NEUWordsCount.java  # 作业1：文本词频统计
│   │   ├── ClassWork2_SQLSum.java         # 作业2：SQL 数据汇总
│   │   └── ClassWork3_StreamCount.java    # 作业3：流数据统计
│   └── resources/
│       ├── 东北大学.txt                   # 作业1输入文本
│       ├── people.csv
│       ├── t_etl2_jjjyqrxx.csv            # 作业2输入数据
│       └── dump-spark_demo.sql
doc/
├── 作业内容.md
├── 实验内容.md
└── 实验数据库信息.md
```

---

## 🧠 技术栈

| 分类           | 技术栈           |
| -------------- | ---------------- |
| **语言**       | Java 1.8         |
| **大数据框架** | Apache Spark 3.5 |
| **数据库**     | MySQL 8.0        |
| **依赖管理**   | Maven 3.5        |

---

## 🗄️ 数据库配置

项目涉及两个数据库：

| 名称           | 作用           |
| -------------- | -------------- |
| `spark_demo`   | 源数据数据库   |
| `fi_warehouse` | 数据仓库数据库 |

数据库连接示例配置：

```properties
url=jdbc:mysql://localhost:3306/spark_demo?serverTimezone=Asia/Shanghai&useTimezone=true
user=root
password=12345
```

> ⚠️ 注意：若本地数据库配置不同，请根据实际情况修改代码中的连接信息。

---

## 🧰 使用说明

1. 使用 **IntelliJ IDEA** 打开项目；
2. 打开项目目录下的 `pom.xml` 文件，使用 **Maven** 构建项目；
3. 在 `pom.xml` 的 `<manifest>` 中设置主类：

   ```xml
   <manifest>
       <mainClass>com.szkingdom.s20230123.XXX</mainClass>
   </manifest>
   ```

   将 `XXX` 修改为需要运行的类名；

4. 运行项目即可。

---

## ⚠️ 注意事项

- 确保 **MySQL 服务** 已启动并配置正确；
- 在运行 **ClassWork3_StreamCount.java** 前，请先启动 Socket 服务；
- **Windows** 系统可通过安装 **netcat** 工具启动 Socket 流；
- **Linux/macOS** 系统可使用 **telnet** 命令启动。
