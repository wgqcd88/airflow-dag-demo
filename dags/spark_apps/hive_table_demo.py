"""PySpark 建表示例：连 Hive Metastore 建库建表，数据落 ADLS Gen2（Storage Account）。

用法：
    spark-submit hive_table_demo.py --table demo_db.airflow_demo --rows 5

参数（均为命令行参数，不依赖环境变量插值）：
    --table      目标表名（database.table），默认 demo_db.airflow_demo
    --rows       造多少行示例数据，默认 5
    --warehouse  可选，覆盖 spark.sql.warehouse.dir；默认继承会话配置（即 HMS 的 warehouse.dir）

依赖会话侧 spark.conf 已配置：
    - spark.hadoop.hive.metastore.uris = thrift://hive-metastore.data-platform.svc.cluster.local:9083
    - ADLS Gen2（wgqjesa）的 ABFS OAuth（Workload Identity）配置键
这些由提交作业的 SparkApplication CRD 的 sparkConf 注入；本脚本只用 enableHiveSupport()
承接，不在代码里硬编码 HMS/存储地址，便于同一份脚本换环境复用。

建表数据的物理位置由 Hive Metastore 的 warehouse.dir 决定（本环境为
abfss://warehouse@wgqjesa.dfs.core.windows.net/），因此 saveAsTable 会把 parquet
文件写到该 ADLS 容器下的 <db>.db/<table>/ 目录。
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hive Metastore 建表 -> ADLS 示例")
    parser.add_argument("--table", default="demo_db.airflow_demo",
                        help="目标表名 database.table")
    parser.add_argument("--rows", type=int, default=5, help="造多少行示例数据")
    parser.add_argument("--warehouse", default=None,
                        help="可选：覆盖 spark.sql.warehouse.dir")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if "." not in args.table:
        raise SystemExit(f"--table 需为 database.table 形式，收到: {args.table}")
    database, _table = args.table.split(".", 1)

    builder = SparkSession.builder.appName("hive_table_demo").enableHiveSupport()
    if args.warehouse:
        builder = builder.config("spark.sql.warehouse.dir", args.warehouse)
    spark = builder.getOrCreate()

    try:
        wh = spark.conf.get("spark.sql.warehouse.dir", "<unset>")
        uris = spark.conf.get("spark.hadoop.hive.metastore.uris", "<unset>")
        print(f"[hive_table_demo] warehouse.dir={wh} metastore.uris={uris}")

        # 1. 建库
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

        # 2. 造几行示例数据
        rows = [(i, f"name_{i}", float(i) * 1.5) for i in range(1, args.rows + 1)]
        df = spark.createDataFrame(rows, schema=["id", "name", "score"])

        # 3. 写成 Hive 表（数据落到 HMS warehouse = ADLS）；overwrite 便于幂等重跑
        df.write.mode("overwrite").format("parquet").saveAsTable(args.table)
        print(f"[hive_table_demo] 已写表 {args.table}（{df.count()} 行）")

        # 4. 回读自证 + 打印表的物理位置（应指向 abfss://）
        back = spark.table(args.table)
        back.show(truncate=False)
        loc = (
            spark.sql(f"DESCRIBE FORMATTED {args.table}")
            .filter("col_name = 'Location'")
            .collect()
        )
        location = loc[0]["data_type"].strip() if loc else "<unknown>"
        print(f"[hive_table_demo] 建表完成 table={args.table} count={back.count()} "
              f"location={location}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
