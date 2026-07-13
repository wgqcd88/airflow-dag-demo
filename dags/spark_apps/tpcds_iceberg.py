"""用 Kyuubi TPC-DS connector 生成数据，CTAS 写入 Iceberg 表（Hive catalog，落 ADLS）。

用法：
    spark-submit tpcds_iceberg.py --scale 1 --db tpcds_ice_sf1 --tables all

Kyuubi TPC-DS connector 注册为 catalog `tpcds`，其 database 按 scale 命名为 `sf<scale>`。
表是「虚拟」的，SELECT 时按 scale 现算生成数据；本作业把它们 CTAS 成 Iceberg 表
（catalog `ice`，type=hive，元数据走共享 HMS，数据落 ADLS warehouse 的 iceberg 前缀）。

catalog 约定（由 SparkApplication 的 sparkConf 注入，本脚本只使用）：
    - tpcds : Kyuubi TPC-DS connector（spark.jars.packages + spark.sql.catalog.tpcds=...）
    - ice   : Iceberg SparkCatalog，type=hive；表名 ice.<db>.<table>

参数（命令行）：
    --scale   scale factor（默认 1）；对应 tpcds.sf<scale> database。
    --db      目标 Iceberg database（默认 tpcds_ice_sf<scale>）；建在 ice catalog 下。
    --tables  要写入的表：all（全部 24 表）或逗号分隔子集（如 store_sales,item,customer）。
"""

from __future__ import annotations

import argparse
import traceback

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType

ICE_CATALOG = "ice"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Kyuubi TPC-DS -> Iceberg (Hive catalog)")
    p.add_argument("--scale", default="1", help="scale factor，对应 tpcds.sf<scale>")
    p.add_argument("--db", default=None, help="目标 Iceberg database，默认 tpcds_ice_sf<scale>")
    p.add_argument("--tables", default="all", help="all 或逗号分隔表名子集")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    scale = args.scale.strip()
    src_db = f"tpcds.sf{scale}"                            # kyuubi tpcds 的 scale database
    dst_db = args.db or f"tpcds_ice_sf{scale}"             # 目标 Iceberg database（ice catalog 下）

    spark = SparkSession.builder.appName("tpcds_iceberg").enableHiveSupport().getOrCreate()

    # 列出 tpcds catalog 下该 scale 的所有表
    all_tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {src_db}").collect()]
    print(f"[tpcds_iceberg] catalog {src_db} 下共 {len(all_tables)} 张表: {all_tables}")

    if args.tables.strip().lower() == "all":
        tables = all_tables
    else:
        want = [t.strip() for t in args.tables.split(",") if t.strip()]
        tables = [t for t in want if t in all_tables]
        missing = [t for t in want if t not in all_tables]
        if missing:
            print(f"[tpcds_iceberg] 警告：以下表不在 catalog 中，跳过: {missing}")

    # 目标 database 建在 Iceberg catalog ice 下
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {ICE_CATALOG}.{dst_db}")

    results = []
    for t in tables:
        src = f"{src_db}.{t}"
        dst = f"{ICE_CATALOG}.{dst_db}.{t}"
        try:
            spark.sql(f"DROP TABLE IF EXISTS {dst}")
            # 不用纯 SQL CTAS：Kyuubi TPC-DS connector 把部分列标成 non-nullable(required),
            # 但 SCD 维表(如 item)历史行这些列实际为 null，Iceberg 写 required 字段遇 null 会
            # NullPointerException。故读源表后把所有列强制改为 nullable，再建 Iceberg 表。
            df = spark.table(src)
            nullable_schema = StructType(
                [StructField(f.name, f.dataType, True, f.metadata) for f in df.schema.fields]
            )
            df = spark.createDataFrame(df.rdd, nullable_schema)
            df.writeTo(dst).using("iceberg").create()
            cnt = spark.table(dst).count()
            print(f"[tpcds_iceberg] OK {dst} rows={cnt}")
            results.append((t, cnt, None))
        except Exception as e:  # 单表失败不影响其它
            print(f"[tpcds_iceberg] FAILED {dst}: {e}")
            traceback.print_exc()
            results.append((t, -1, str(e)))

    print(f"[tpcds_iceberg] ===== 汇总 (scale=sf{scale} -> {ICE_CATALOG}.{dst_db}) =====")
    ok = 0
    total_rows = 0
    for t, cnt, err in results:
        if err is None:
            ok += 1
            total_rows += cnt
            print(f"[tpcds_iceberg]   OK   {t} rows={cnt}")
        else:
            print(f"[tpcds_iceberg]   FAIL {t}: {err}")
    print(f"[tpcds_iceberg] 成功 {ok}/{len(results)} 张表，总行数 {total_rows}")

    spark.stop()
    if ok != len(results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
