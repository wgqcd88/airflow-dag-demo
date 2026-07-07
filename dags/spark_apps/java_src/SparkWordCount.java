package com.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.split;

/**
 * Spark WordCount 应用（Java 版）。
 *
 * 用法：
 *     spark-submit --class com.example.spark.SparkWordCount wordcount-app.jar <input> <output>
 *
 * <input>  读取的文本路径（file:// / abfss:// / http 已由容器脚本预下载到本地）。
 * <output> 结果输出目录（CSV，overwrite，带表头）。
 *
 * input/output 作为命令行位置参数传入（application args），不再依赖环境变量插值。
 * Spark master 由 spark-submit 的 --master 指定，代码不硬编码，因此同一 jar
 * 可在 local / standalone / k8s / yarn 上运行。
 */
public final class SparkWordCount {

    private SparkWordCount() {
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            throw new IllegalArgumentException(
                    "用法: SparkWordCount <input> <output>（需要两个位置参数）");
        }
        final String inputPath = args[0];
        final String outputPath = args[1];

        final SparkSession spark = SparkSession.builder()
                .appName("SparkWordCount")
                .getOrCreate();
        try {
            final Dataset<Row> lines = spark.read().text(inputPath);
            final Dataset<Row> counts = lines
                    .select(explode(split(col("value"), "\\s+")).alias("word"))
                    .filter(col("word").notEqual(""))
                    .groupBy(col("word"))
                    .count()
                    .orderBy(col("count").desc());

            counts.show(20, false);
            counts.coalesce(1)
                    .write()
                    .mode("overwrite")
                    .option("header", true)
                    .csv(outputPath);

            System.out.println("WordCount 完成：" + counts.count()
                    + " 个不同单词，结果写入 " + outputPath);
        } finally {
            spark.stop();
        }
    }
}
