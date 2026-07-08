"""PyFlink 流作业：datagen 源 -> Paimon 表（Hive catalog）。

用 Flink Table API/SQL 创建一个 datagen 源表（持续生成数据的内置连接器），
和一个 Paimon catalog（metastore=hive，元数据走 Hive Metastore，数据落 ADLS Gen2），
然后流式 INSERT 把 datagen 数据写入 Paimon 表。

Application 模式运行：FlinkDeployment 的 jobManager 直接执行本文件的 main。

Paimon + Hive catalog 约定（由 FlinkDeployment 的 flinkConfiguration / 本脚本 SQL 指定）：
    - catalog 类型 paimon，metastore=hive，uri 指向 HMS thrift，warehouse 落 abfss://
    - ADLS 用 Workload Identity（azure-fs-hadoop 插件 + fs.azure OAuth conf，由 FlinkDeployment 注入）

环境变量（FlinkDeployment 注入，带默认值便于本地/调试）：
    HIVE_METASTORE_URIS / PAIMON_WAREHOUSE / PAIMON_DB / PAIMON_TABLE / GEN_ROWS_PER_SEC
"""

from __future__ import annotations

import os

from pyflink.table import EnvironmentSettings, TableEnvironment


def main() -> None:
    hms_uris = os.getenv(
        "HIVE_METASTORE_URIS",
        "thrift://hive-metastore.data-platform.svc.cluster.local:9083",
    )
    warehouse = os.getenv(
        "PAIMON_WAREHOUSE", "abfss://warehouse@wgqjesa.dfs.core.windows.net/paimon"
    )
    db = os.getenv("PAIMON_DB", "flink_demo")
    table = os.getenv("PAIMON_TABLE", "orders_stream")
    rows_per_sec = os.getenv("GEN_ROWS_PER_SEC", "5")

    # 流模式 TableEnvironment
    env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    # checkpoint：Paimon 依赖 checkpoint 提交快照
    env.get_config().get_configuration().set_string(
        "execution.checkpointing.interval", "30 s"
    )

    # 1. Paimon catalog（Hive metastore）
    env.execute_sql(
        f"""
        CREATE CATALOG paimon WITH (
            'type' = 'paimon',
            'metastore' = 'hive',
            'uri' = '{hms_uris}',
            'warehouse' = '{warehouse}'
        )
        """
    )
    env.execute_sql("USE CATALOG paimon")
    env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {db}")

    # 2. Paimon 目标表（若不存在则建）
    env.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS {db}.{table} (
            id BIGINT,
            item_id INT,
            price DECIMAL(10, 2),
            order_time TIMESTAMP(3),
            PRIMARY KEY (id) NOT ENFORCED
        ) WITH (
            'bucket' = '2'
        )
        """
    )

    # 3. datagen 源表（内置连接器，持续生成数据）——建在默认内存 catalog 下
    env.execute_sql("CREATE CATALOG dgcat WITH ('type' = 'generic_in_memory')")
    env.execute_sql("USE CATALOG dgcat")
    env.execute_sql(
        f"""
        CREATE TEMPORARY TABLE datagen_src (
            id BIGINT,
            item_id INT,
            price DECIMAL(10, 2),
            order_time AS LOCALTIMESTAMP
        ) WITH (
            'connector' = 'datagen',
            'rows-per-second' = '{rows_per_sec}',
            'fields.id.kind' = 'sequence',
            'fields.id.start' = '1',
            'fields.id.end' = '1000000',
            'fields.item_id.min' = '1',
            'fields.item_id.max' = '1000',
            'fields.price.min' = '1.00',
            'fields.price.max' = '9999.99'
        )
        """
    )

    # 4. 流式 INSERT：datagen -> Paimon 表（跨 catalog 用全限定名）
    stmt = (
        f"INSERT INTO paimon.{db}.{table} "
        f"SELECT id, item_id, CAST(price AS DECIMAL(10,2)), order_time FROM dgcat.default_database.datagen_src"
    )
    print(f"[datagen_to_paimon] 提交流作业: {stmt}")
    print(f"[datagen_to_paimon] catalog=paimon(hive) warehouse={warehouse} "
          f"target=paimon.{db}.{table} rows/s={rows_per_sec}")
    env.execute_sql(stmt).wait()  # 流作业，持续运行


if __name__ == "__main__":
    main()
