"""通过 SparkKubernetesOperator（Spark Operator / SparkApplication CRD）调度 PySpark 作业：
用 Kyuubi TPC-DS connector 生成数据，CTAS 写入 Iceberg 表（Hive catalog，数据落 ADLS）。

与 spark_tpcds_gen_sparkop.py 的区别：目标表格式从 parquet Hive 表改为 Iceberg 表
（catalog `ice`，type=hive，元数据走共享 HMS，数据落 ADLS warehouse 的 iceberg 前缀）。

catalog：
    - tpcds : Kyuubi TPC-DS connector，spark.jars.packages 运行时从 Maven 拉，注册为 catalog `tpcds`
    - ice   : Iceberg SparkCatalog，type=hive，共享 HMS

镜像用带 python 的 `3.5.4-gluten-pyspark-lakerss`（Spark 3.5 / Scala 2.12，内含
iceberg-spark-runtime）。Kyuubi TPC-DS connector 用 2.12 版对齐。

参数（触发时可覆盖）：
    - scale        TPC-DS scale factor（默认 1），对应 tpcds.sf<scale>
    - db           目标 Iceberg database（默认空 -> 应用侧用 tpcds_ice_sf<scale>）
    - tables       all 或逗号分隔表名子集
    - app_file_url PySpark 主文件地址（默认 main 分支 raw；raw CDN 缓存时用 commit-SHA）
"""

from __future__ import annotations

import os

import pendulum

try:
    from airflow.sdk import DAG, Param
except ImportError:
    from airflow.models.dag import DAG
    from airflow.models.param import Param

try:
    from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
        SparkKubernetesOperator,
    )
except ImportError:
    from airflow.providers.cncf.kubernetes.operators.spark_kubernetes_operator import (
        SparkKubernetesOperator,
    )

from spark_resource_params import (
    RESOURCE_PARAMS,
    driver_spec,
    executor_spec,
    resource_spark_conf,
)

JOB_NAMESPACE = os.getenv("SPARK_JOB_NAMESPACE", "data-platform")

# 带 python 的 Spark 3.5 lakerss 镜像（Scala 2.12，含 iceberg-spark-runtime）。
IMAGE = os.getenv(
    "SPARK_TPCDS_ICEBERG_IMAGE",
    "ghcr.io/wgqcd88/spark:3.5.4-gluten-pyspark-lakerss-20260712",
)
# Kyuubi TPC-DS connector Maven 坐标（Scala 2.12 配 Spark 3.5）。
KYUUBI_TPCDS_PACKAGE = os.getenv(
    "KYUUBI_TPCDS_PACKAGE",
    "org.apache.kyuubi:kyuubi-spark-connector-tpcds_2.12:1.11.1",
)

DEFAULT_APP_FILE_URL = os.getenv(
    "SPARK_ICEBERG_APP_FILE_URL",
    "https://raw.githubusercontent.com/wgqcd88/airflow-dag-demo/main/dags/spark_apps/tpcds_iceberg.py",
)

HMS_URIS = os.getenv(
    "HIVE_METASTORE_URIS",
    "thrift://hive-metastore.data-platform.svc.cluster.local:9083",
)
ADLS_HOST = os.getenv("SPARK_ADLS_HOST", "wgqjesa.dfs.core.windows.net")
WAREHOUSE_DIR = os.getenv(
    "SPARK_WAREHOUSE_DIR", "abfss://warehouse@wgqjesa.dfs.core.windows.net/"
)
ICEBERG_WAREHOUSE = os.getenv(
    "SPARK_ICEBERG_WAREHOUSE",
    "abfss://warehouse@wgqjesa.dfs.core.windows.net/iceberg",
)
EVENTLOG_DIR = os.getenv(
    "SPARK_EVENTLOG_DIR", "abfss://logs@wgqjesa.dfs.core.windows.net/spark-events"
)
AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID", "16b3c013-d300-468d-ac64-7eda0820b6d3")
AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID", "8b8e1707-3dd1-4942-a0f6-9f5038b5c74e")
AZURE_FEDERATED_TOKEN_FILE = os.getenv(
    "AZURE_FEDERATED_TOKEN_FILE", "/var/run/secrets/azure/tokens/azure-identity-token"
)
SERVICE_ACCOUNT = os.getenv("SPARK_SERVICE_ACCOUNT", "spark-sa")


def _abfs_oauth_conf(host: str) -> dict:
    return {
        f"spark.hadoop.fs.azure.account.auth.type.{host}": "OAuth",
        f"spark.hadoop.fs.azure.account.oauth.provider.type.{host}":
            "org.apache.hadoop.fs.azurebfs.oauth2.WorkloadIdentityTokenProvider",
        f"spark.hadoop.fs.azure.account.oauth2.msi.tenant.{host}": AZURE_TENANT_ID,
        f"spark.hadoop.fs.azure.account.oauth2.client.id.{host}": AZURE_CLIENT_ID,
        f"spark.hadoop.fs.azure.account.oauth2.token.file.{host}": AZURE_FEDERATED_TOKEN_FILE,
        f"spark.hadoop.fs.azure.account.hns.enabled.{host}": "true",
    }


def build_spark_application() -> dict:
    spark_conf = {
        "spark.hadoop.hive.metastore.uris": HMS_URIS,
        "spark.sql.warehouse.dir": WAREHOUSE_DIR,
        "spark.sql.catalogImplementation": "hive",
        # 运行时从 Maven 拉 Kyuubi TPC-DS connector，并注册为 catalog `tpcds`。
        "spark.jars.packages": KYUUBI_TPCDS_PACKAGE,
        # Spark Operator 的 pod HOME=/nonexistent 不可写，显式指到可写的 /tmp。
        "spark.jars.ivy": "/tmp/.ivy2",
        "spark.sql.catalog.tpcds": "org.apache.kyuubi.spark.connector.tpcds.TPCDSCatalog",
        # Iceberg：extensions + catalog `ice`（type=hive，共享 HMS，warehouse 落 ADLS）。
        "spark.sql.extensions":
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.ice": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.ice.type": "hive",
        "spark.sql.catalog.ice.uri": HMS_URIS,
        "spark.sql.catalog.ice.warehouse": ICEBERG_WAREHOUSE,
        # event log -> ADLS，接 History Server。
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": EVENTLOG_DIR,
        # Gluten 计算加速 + 全局禁用原生 IO（ADLS 读写走 JVM Hadoop ABFS/WI）。
        "spark.plugins": "org.apache.gluten.GlutenPlugin",
        "spark.memory.offHeap.enabled": "true",
        "spark.memory.offHeap.size": "2g",
        "spark.gluten.sql.columnar.filescan": "false",
        "spark.gluten.sql.columnar.batchscan": "false",
        "spark.gluten.sql.columnar.hivetablescan": "false",
        "spark.gluten.sql.native.writer.enabled": "false",
        "spark.gluten.sql.native.hive.writer.enabled": "false",
    }
    spark_conf.update(_abfs_oauth_conf(ADLS_HOST))
    spark_conf.update(resource_spark_conf())

    labels = {"azure.workload.identity/use": "true"}
    return {
        "spark": {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "metadata": {"name": "tpcds-iceberg", "namespace": JOB_NAMESPACE},
            "spec": {
                "type": "Python",
                "pythonVersion": "3",
                "mode": "cluster",
                "image": IMAGE,
                "imagePullPolicy": "IfNotPresent",
                "mainApplicationFile": "{{ params.app_file_url }}",
                "arguments": [
                    "--scale", "{{ params.scale }}",
                    "--db", "{{ params.db }}",
                    "--tables", "{{ params.tables }}",
                ],
                "sparkVersion": "3.5.4",
                "sparkConf": spark_conf,
                "restartPolicy": {"type": "Never"},
                "driver": driver_spec(service_account=SERVICE_ACCOUNT, labels=labels),
                "executor": executor_spec(labels=labels),
            },
        }
    }


with DAG(
    dag_id="spark_tpcds_iceberg_sparkop",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["demo", "spark", "spark-operator", "tpcds", "iceberg"],
    params={
        "scale": Param("1", type="string", title="Scale factor",
                       description="TPC-DS scale，对应 tpcds.sf<scale>（如 1 / 10）"),
        "db": Param("", type="string", title="目标 Iceberg database",
                    description="留空则用 tpcds_ice_sf<scale>（建在 ice catalog 下）"),
        "tables": Param("all", type="string", title="表",
                        description="all 或逗号分隔表名子集，如 store_sales,item,customer"),
        "app_file_url": Param(
            DEFAULT_APP_FILE_URL, type="string", title="PySpark 主文件 URL",
            description="默认 main 分支 raw URL；raw CDN 有缓存，紧急用 commit-SHA 路径。",
        ),
        **{**RESOURCE_PARAMS, **{
            "executor_cpu": Param("2", type="string", title="Executor CPU",
                                  description="每个 executor 的 CPU，如 1 / 2 / 4"),
            "executor_memory": Param("4g", type="string", title="Executor 内存",
                                     description="每个 executor 内存，如 4g / 8g"),
            "executor_instances": Param("2", type="string", title="Executor 实例数",
                                        description="executor 数量（大 scale 可调大）"),
        }},
    },
) as dag:
    submit = SparkKubernetesOperator(
        task_id="submit_tpcds_iceberg",
        namespace=JOB_NAMESPACE,
        template_spec=build_spark_application(),
        get_logs=True,
        delete_on_termination=False,
        do_xcom_push=False,
    )
