package com.example.spark;

import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

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

    /**
     * http(s) 输入下载到 driver 本地临时文件并返回其 file:// URI；其余 scheme 原样返回。
     */
    private static String resolveInput(String input) {
        if (input.startsWith("http://") || input.startsWith("https://")) {
            try {
                Path tmp = Files.createTempFile("spark_wc_input", ".txt");
                try (InputStream in = new URL(input).openStream()) {
                    Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING);
                }
                System.out.println("已下载 http 输入到本地: " + tmp);
                return tmp.toUri().toString();
            } catch (Exception e) {
                throw new RuntimeException("下载 http(s) 输入失败: " + input, e);
            }
        }
        return input;
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            throw new IllegalArgumentException(
                    "用法: SparkWordCount <input> <output>（需要两个位置参数）");
        }
        final String outputPath = args[1];
        // Spark 的 read().text() 不支持 http(s) scheme（Hadoop FS 无此 scheme）。
        // 若 input 是 http(s) URL，先由 driver 下载到本地临时文件再读；
        // file:// / abfss:// / hdfs:// 等路径直接透传给 Spark。
        final String inputPath = resolveInput(args[0]);

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
