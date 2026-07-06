"""Spark WordCount 应用：读取 <input>，统计词频，写出 <output>（CSV，overwrite）。

用法：
    spark-submit wordcount.py <input> <output>

Spark master 由 spark-submit 的 --master 指定（本脚本用 getOrCreate 继承，
不在代码里硬编码），因此同一份应用可在 local / standalone / k8s / yarn 上运行。
"""

from __future__ import annotations

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    if len(sys.argv) < 3:
        raise SystemExit("用法: wordcount.py <input> <output>")
    input_path, output_path = sys.argv[1], sys.argv[2]

    spark = SparkSession.builder.appName("wordcount").getOrCreate()
    try:
        lines = spark.read.text(input_path)
        counts = (
            lines.select(F.explode(F.split(F.col("value"), r"\s+")).alias("word"))
            .filter(F.col("word") != "")
            .groupBy("word")
            .count()
            .orderBy(F.col("count").desc())
        )
        counts.show(20, truncate=False)
        counts.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
        print(f"WordCount 完成：{counts.count()} 个不同单词，结果写入 {output_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
