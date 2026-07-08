"""用 Kyuubi TPC-DS connector 生成数据，CTAS 成 Hive Metastore 实体表（parquet，落 ADLS）。

用法：
    spark-submit tpcds_gen.py --scale 1 --db tpcds_sf1 --tables all

Kyuubi TPC-DS connector（org.apache.kyuubi:kyuubi-spark-connector-tpcds）注册为
catalog `tpcds`，其 database 按 scale factor 命名为 `sf<scale>`（如 sf1 / sf10），
另有 `tiny`。表是「虚拟」的，SELECT 时按 scale 现算生成数据；本作业把它们 CTAS
成真正的 Hive 表（物理 parquet 文件落 ADLS warehouse）。

参数（命令行）：
    --scale   scale factor（默认 1）；对应 tpcds.sf<scale> database。
    --db      目标实体表所在 database（默认 tpcds_sf<scale>）。
    --tables  要生成的表：all（全部 24 表）或逗号分隔子集（如 store_sales,item,customer）。

catalog `tpcds` 由 SparkApplication 的 sparkConf 注册（spark.jars.packages 拉 connector jar +
spark.sql.catalog.tpcds=...TPCDSCatalog）。本脚本只使用，不硬编码 connector 坐标。
"""

from __future__ import annotations

import argparse
import traceback

from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Kyuubi TPC-DS -> HMS 实体表")
    p.add_argument("--scale", default="1", help="scale factor，对应 tpcds.sf<scale>")
    p.add_argument("--db", default=None, help="目标 database，默认 tpcds_sf<scale>")
    p.add_argument("--tables", default="all",
                   help="all 或逗号分隔表名子集")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    scale = args.scale.strip()
    src_db = f"tpcds.sf{scale}"                       # kyuubi tpcds 的 scale database
    dst_db = args.db or f"tpcds_sf{scale}"            # 目标实体表 database

    spark = SparkSession.builder.appName("tpcds_gen").enableHiveSupport().getOrCreate()

    # 列出 tpcds catalog 下该 scale 的所有表
    all_tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {src_db}").collect()]
    print(f"[tpcds_gen] catalog {src_db} 下共 {len(all_tables)} 张表: {all_tables}")

    if args.tables.strip().lower() == "all":
        tables = all_tables
    else:
        want = [t.strip() for t in args.tables.split(",") if t.strip()]
        tables = [t for t in want if t in all_tables]
        missing = [t for t in want if t not in all_tables]
        if missing:
            print(f"[tpcds_gen] 警告：以下表不在 catalog 中，跳过: {missing}")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {dst_db}")

    results = []
    for t in tables:
        src = f"{src_db}.{t}"
        dst = f"{dst_db}.{t}"
        try:
            spark.sql(f"DROP TABLE IF EXISTS {dst}")
            # CTAS：把虚拟 tpcds 表物化成 Hive parquet 实体表
            spark.sql(
                f"CREATE TABLE {dst} USING parquet AS SELECT * FROM {src}"
            )
            cnt = spark.table(dst).count()
            print(f"[tpcds_gen] OK {dst} rows={cnt}")
            results.append((t, cnt, None))
        except Exception as e:  # 单表失败不影响其它
            print(f"[tpcds_gen] FAILED {dst}: {e}")
            traceback.print_exc()
            results.append((t, -1, str(e)))

    print(f"[tpcds_gen] ===== 汇总 (scale=sf{scale} -> {dst_db}) =====")
    ok = 0
    total_rows = 0
    for t, cnt, err in results:
        if err is None:
            ok += 1
            total_rows += cnt
            print(f"[tpcds_gen]   OK   {t} rows={cnt}")
        else:
            print(f"[tpcds_gen]   FAIL {t}: {err}")
    print(f"[tpcds_gen] 成功 {ok}/{len(results)} 张表，总行数 {total_rows}")

    spark.stop()
    if ok != len(results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
