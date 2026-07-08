"""四数据湖建表示例：一个 PySpark 作业内分步给 Iceberg / Delta / Hudi / Paimon
各建一张表并写入数据，四湖元数据统一走 Hive Metastore，数据落 ADLS Gen2。

用法：
    spark-submit four_lakes_demo.py --db lake_demo --rows 5 --formats iceberg,delta,hudi,paimon

参数（命令行）：
    --db       目标 database 名（默认 lake_demo）；各湖在各自 catalog 下建同名 db。
    --rows     造多少行示例数据（默认 5）。
    --formats  逗号分隔，要建的格式子集（默认 iceberg,delta,hudi,paimon）。

catalog 约定（由 SparkApplication 的 sparkConf 注入，本脚本只使用）：
    - Iceberg : 独立 catalog `ice`（type=hive），表名 ice.<db>.t_iceberg
    - Paimon  : 独立 catalog `paimon`（metastore=hive），表名 paimon.<db>.t_paimon
    - Delta   : 默认 spark_catalog（DeltaCatalog），表名 <db>.t_delta，USING delta
    - Hudi    : 默认 spark_catalog（Delta 已接管），故用 DataFrameWriter format("hudi")
                + hive_sync 落 HMS，表名 <db>.t_hudi

Delta 与 Hudi 都想接管默认 spark_catalog（互斥），本脚本让 Delta 接管、Hudi 走 datasource
写入并显式开启 hive sync，从而在同一个 SparkSession 内四种格式都能建表。每种格式独立
try/except，一种失败不影响其它，末尾打印汇总。
"""

from __future__ import annotations

import argparse
import traceback

from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="四数据湖建表 -> HMS + ADLS")
    p.add_argument("--db", default="lake_demo")
    p.add_argument("--rows", type=int, default=5)
    p.add_argument("--formats", default="iceberg,delta,hudi,paimon")
    return p.parse_args()


def make_df(spark, rows: int):
    data = [(i, f"name_{i}", float(i) * 1.5) for i in range(1, rows + 1)]
    return spark.createDataFrame(data, schema=["id", "name", "score"])


def do_iceberg(spark, db: str, df) -> str:
    # Iceberg 独立 catalog ice（type=hive），db/表都在 ice 下。
    spark.sql(f"CREATE DATABASE IF NOT EXISTS ice.{db}")
    tbl = f"ice.{db}.t_iceberg"
    spark.sql(f"DROP TABLE IF EXISTS {tbl}")
    df.writeTo(tbl).using("iceberg").create()
    cnt = spark.table(tbl).count()
    loc = _location(spark, tbl)
    return f"iceberg OK table={tbl} count={cnt} location={loc}"


def do_delta(spark, db: str, df) -> str:
    # Delta 走默认 spark_catalog（DeltaCatalog）。
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    tbl = f"{db}.t_delta"
    df.write.format("delta").mode("overwrite").saveAsTable(tbl)
    cnt = spark.table(tbl).count()
    loc = _location(spark, tbl)
    return f"delta OK table={tbl} count={cnt} location={loc}"


def do_hudi(spark, db: str, df) -> str:
    # Hudi 不接管 spark_catalog（让给 Delta），用 datasource 写 + 显式 hive sync 到 HMS。
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    tbl = f"{db}.t_hudi"
    hms = spark.conf.get("spark.hadoop.hive.metastore.uris")
    (
        df.write.format("hudi")
        .option("hoodie.table.name", "t_hudi")
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.datasource.write.precombine.field", "score")
        .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
        # 同步到 Hive Metastore（用 hms mode，直连 thrift）
        .option("hoodie.datasource.hive_sync.enable", "true")
        .option("hoodie.datasource.hive_sync.mode", "hms")
        .option("hoodie.datasource.hive_sync.metastore.uris", hms)
        .option("hoodie.datasource.hive_sync.database", db)
        .option("hoodie.datasource.hive_sync.table", "t_hudi")
        .mode("overwrite")
        .saveAsTable(tbl)
    )
    cnt = spark.table(tbl).count()
    loc = _location(spark, tbl)
    return f"hudi OK table={tbl} count={cnt} location={loc}"


def do_paimon(spark, db: str, df) -> str:
    # Paimon 独立 catalog paimon（metastore=hive）。
    spark.sql(f"CREATE DATABASE IF NOT EXISTS paimon.{db}")
    tbl = f"paimon.{db}.t_paimon"
    spark.sql(f"DROP TABLE IF EXISTS {tbl}")
    df.writeTo(tbl).using("paimon").create()
    cnt = spark.table(tbl).count()
    loc = _location(spark, tbl)
    return f"paimon OK table={tbl} count={cnt} location={loc}"


def _location(spark, tbl: str) -> str:
    try:
        rows = (
            spark.sql(f"DESCRIBE FORMATTED {tbl}")
            .filter("col_name like '%Location%' or col_name like '%location%'")
            .collect()
        )
        for r in rows:
            v = (r["data_type"] or "").strip()
            if v.startswith("abfss://") or v.startswith("file:"):
                return v
    except Exception:
        pass
    return "<unknown>"


HANDLERS = {
    "iceberg": do_iceberg,
    "delta": do_delta,
    "hudi": do_hudi,
    "paimon": do_paimon,
}


def main() -> None:
    args = parse_args()
    formats = [f.strip().lower() for f in args.formats.split(",") if f.strip()]

    spark = SparkSession.builder.appName("four_lakes_demo").enableHiveSupport().getOrCreate()
    df = make_df(spark, args.rows)

    results = []
    for fmt in formats:
        handler = HANDLERS.get(fmt)
        if handler is None:
            results.append(f"{fmt} SKIP 未知格式")
            continue
        try:
            msg = handler(spark, args.db, df)
            print(f"[four_lakes_demo] {msg}")
            results.append(msg)
        except Exception as e:  # 单个格式失败不影响其它
            print(f"[four_lakes_demo] {fmt} FAILED: {e}")
            traceback.print_exc()
            results.append(f"{fmt} FAILED: {e}")

    print("[four_lakes_demo] ===== 汇总 =====")
    for r in results:
        print(f"[four_lakes_demo]   {r}")
    spark.stop()

    # 任一格式失败则非零退出，让 SparkApplication 标记为 FAILED（便于发现问题）。
    if any("FAILED" in r for r in results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
