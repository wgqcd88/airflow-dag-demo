package com.example.spark;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
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
 * <input>  读取的文本路径：http(s) URL / file:// / abfss:// / hdfs:// 均可。
 * <output> 结果输出目录（CSV，overwrite，带表头）。
 *
 * input/output 作为命令行位置参数传入（application args），不依赖环境变量插值。
 * Spark master 由 spark-submit 的 --master 指定，代码不硬编码，因此同一 jar
 * 可在 local / standalone / k8s / yarn 上运行。
 *
 * http(s) 输入：Hadoop FS 无 http scheme，且 cluster mode 下 executor 与 driver 不共享
 * 本地磁盘，故不能「driver 下载到本地文件再让 executor 读」。这里由 driver 把内容读进内存，
 * 用 spark.createDataset 并行化成 Dataset（数据随任务分发到 executor）。其余 scheme 走
 * spark.read().text()（路径对所有 executor 可见，分布式读取）。
 */
public final class SparkWordCount {

    private SparkWordCount() {
    }

    /** http(s) 输入：driver 读进内存为按行的字符串列表。 */
    private static List<String> downloadLines(String input) {
        List<String> lines = new ArrayList<>();
        try (InputStream in = new URL(input).openStream();
                BufferedReader br = new BufferedReader(
                        new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                lines.add(line);
            }
        } catch (Exception e) {
            throw new RuntimeException("下载 http(s) 输入失败: " + input, e);
        }
        System.out.println("已下载 http 输入：" + lines.size() + " 行 <- " + input);
        return lines;
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
            // http(s) 输入用内存并行化，其余 scheme 直接分布式读。
            final Dataset<Row> lines;
            if (inputPath.startsWith("http://") || inputPath.startsWith("https://")) {
                // createDataset(List<String>) 的单列默认名即为 "value"，与 read().text() 一致。
                lines = spark
                        .createDataset(downloadLines(inputPath), Encoders.STRING())
                        .toDF("value");
            } else {
                lines = spark.read().text(inputPath);
            }

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
