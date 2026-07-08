"""通过 SparkKubernetesOperator（Spark Operator / SparkApplication CRD）调度 PySpark 作业：
在一个作业内分步给 Iceberg / Delta / Hudi / Paimon 四种数据湖格式各建一张表并写数据，
四湖元数据统一走 Hive Metastore，数据落 ADLS Gen2。

与 spark_hive_ctas_sparkop.py 同套路（SparkApplication CRD + Gluten 全局禁用原生 IO + WI），
区别在 sparkConf 注入了四种湖格式的 catalog / extensions 配置，镜像用带数据湖 jar 的
`spark:<ver>-gluten-pyspark-lakerss-20260707`（含 Iceberg/Delta/Hudi/Paimon 四格式，版本随 Spark 匹配）。

catalog 约定：
    - Iceberg : 独立 catalog `ice`，type=hive（元数据走 HMS）
    - Paimon  : 独立 catalog `paimon`，metastore=hive
    - Delta   : 默认 spark_catalog（DeltaCatalog）
    - Hudi    : 不接管 spark_catalog（让给 Delta），应用侧用 datasource + hive_sync 落 HMS
    - extensions 合并挂载 Delta/Iceberg/Paimon/Hudi 四个 SessionExtension

参数（触发时可覆盖）：
    - image        : 三选（3.4.4 / 3.5.4 / 4.0.1 的 gluten-pyspark-lakerss 镜像），默认 3.5.4
    - app_file_url : PySpark 主文件地址（默认 main 分支 raw URL；注意 raw CDN 缓存，紧急用 commit-SHA）
    - db           : 目标 database（默认 lake_demo）
    - rows         : 造多少行示例数据
    - formats      : 逗号分隔的格式子集（默认四种全建）
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

JOB_NAMESPACE = os.getenv("SPARK_JOB_NAMESPACE", "data-platform")

# 带数据湖 jar + python 的 gluten-pyspark-lakerss 镜像。
IMAGE_CHOICES = (
    "ghcr.io/wgqcd88/spark:3.4.4-gluten-pyspark-lakerss-20260707",
    "ghcr.io/wgqcd88/spark:3.5.4-gluten-pyspark-lakerss-20260707",
    "ghcr.io/wgqcd88/spark:4.0.1-gluten-pyspark-lakerss-20260707",
)
DEFAULT_IMAGE = os.getenv("SPARK_LAKERSS_IMAGE", IMAGE_CHOICES[1])  # 默认 3.5.4

DEFAULT_APP_FILE_URL = os.getenv(
    "SPARK_APP_FILE_URL",
    "https://raw.githubusercontent.com/wgqcd88/airflow-dag-demo/main/dags/spark_apps/four_lakes_demo.py",
)

HMS_URIS = os.getenv(
    "HIVE_METASTORE_URIS",
    "thrift://hive-metastore.data-platform.svc.cluster.local:9083",
)
ADLS_HOST = os.getenv("SPARK_ADLS_HOST", "wgqjesa.dfs.core.windows.net")
WAREHOUSE_BASE = os.getenv(
    "SPARK_WAREHOUSE_BASE", "abfss://warehouse@wgqjesa.dfs.core.windows.net"
)
WAREHOUSE_DIR = WAREHOUSE_BASE + "/"
EVENTLOG_DIR = os.getenv(
    "SPARK_EVENTLOG_DIR", "abfss://logs@wgqjesa.dfs.core.windows.net/spark-events"
)
AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID", "16b3c013-d300-468d-ac64-7eda0820b6d3")
AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID", "8b8e1707-3dd1-4942-a0f6-9f5038b5c74e")
AZURE_FEDERATED_TOKEN_FILE = os.getenv(
    "AZURE_FEDERATED_TOKEN_FILE", "/var/run/secrets/azure/tokens/azure-identity-token"
)
SERVICE_ACCOUNT = os.getenv("SPARK_SERVICE_ACCOUNT", "spark-sa")

DRIVER_CORES = int(os.getenv("SPARK_DRIVER_CORES", "1"))
DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "2g")
EXECUTOR_CORES = int(os.getenv("SPARK_EXECUTOR_CORES", "1"))
EXECUTOR_INSTANCES = int(os.getenv("SPARK_EXECUTOR_INSTANCES", "1"))
EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "2g")


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


def _lake_catalog_conf() -> dict:
    """四种湖格式的 catalog / extensions 配置，元数据统一走 HMS。"""
    return {
        "spark.sql.catalogImplementation": "hive",
        # 四个 SessionExtension 合并挂载（互不冲突）。
        "spark.sql.extensions": ",".join([
            "io.delta.sql.DeltaSparkSessionExtension",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions",
            "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
        ]),
        # Delta 接管默认 spark_catalog（Hudi 让位，走 datasource 写）。
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        # Iceberg 独立 catalog（type=hive）。
        "spark.sql.catalog.ice": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.ice.type": "hive",
        "spark.sql.catalog.ice.uri": HMS_URIS,
        "spark.sql.catalog.ice.warehouse": WAREHOUSE_BASE + "/iceberg",
        # Paimon 独立 catalog（metastore=hive）。
        "spark.sql.catalog.paimon": "org.apache.paimon.spark.SparkCatalog",
        "spark.sql.catalog.paimon.metastore": "hive",
        "spark.sql.catalog.paimon.uri": HMS_URIS,
        "spark.sql.catalog.paimon.warehouse": WAREHOUSE_BASE + "/paimon",
    }


def build_spark_application() -> dict:
    spark_conf = {
        "spark.hadoop.hive.metastore.uris": HMS_URIS,
        "spark.sql.warehouse.dir": WAREHOUSE_DIR,
        # Hudi 强制要求 KryoSerializer（否则 saveAsTable 报 "hoodie only support KryoSerializer"）。
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        # event log -> ADLS，接 Spark History Server。
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": EVENTLOG_DIR,
        # Gluten 计算加速 + 全局禁用原生 IO（ADLS 读写走 JVM Hadoop ABFS/WI）。
        "spark.plugins": "org.apache.gluten.GlutenPlugin",
        "spark.memory.offHeap.enabled": "true",
        "spark.memory.offHeap.size": "1g",
        "spark.gluten.sql.columnar.filescan": "false",
        "spark.gluten.sql.columnar.batchscan": "false",
        "spark.gluten.sql.columnar.hivetablescan": "false",
        "spark.gluten.sql.native.writer.enabled": "false",
        "spark.gluten.sql.native.hive.writer.enabled": "false",
    }
    spark_conf.update(_lake_catalog_conf())
    spark_conf.update(_abfs_oauth_conf(ADLS_HOST))

    labels = {"azure.workload.identity/use": "true"}
    return {
        "spark": {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "metadata": {"name": "four-lakes-demo", "namespace": JOB_NAMESPACE},
            "spec": {
                "type": "Python",
                "pythonVersion": "3",
                "mode": "cluster",
                "image": "{{ params.image }}",
                "imagePullPolicy": "IfNotPresent",
                "mainApplicationFile": "{{ params.app_file_url }}",
                "arguments": [
                    "--db", "{{ params.db }}",
                    "--rows", "{{ params.rows }}",
                    "--formats", "{{ params.formats }}",
                ],
                "sparkVersion": "3.5.4",
                "sparkConf": spark_conf,
                "restartPolicy": {"type": "Never"},
                "driver": {
                    "cores": DRIVER_CORES,
                    "memory": DRIVER_MEMORY,
                    "serviceAccount": SERVICE_ACCOUNT,
                    "labels": labels,
                },
                "executor": {
                    "cores": EXECUTOR_CORES,
                    "instances": EXECUTOR_INSTANCES,
                    "memory": EXECUTOR_MEMORY,
                    "labels": labels,
                },
            },
        }
    }


with DAG(
    dag_id="spark_four_lakes_sparkop",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["demo", "spark", "spark-operator", "lakehouse", "iceberg", "delta", "hudi", "paimon"],
    params={
        "image": Param(
            DEFAULT_IMAGE, type="string", title="镜像",
            description="带数据湖 jar 的 gluten-pyspark-lakerss 镜像（3.4.4/3.5.4/4.0.1）",
            enum=list(IMAGE_CHOICES),
        ),
        "app_file_url": Param(
            DEFAULT_APP_FILE_URL, type="string", title="PySpark 主文件 URL",
            description="默认 main 分支 raw URL；raw CDN 有缓存，紧急用 commit-SHA 路径。",
        ),
        "db": Param("lake_demo", type="string", title="Database",
                    description="各湖在各自 catalog 下建同名 db"),
        "rows": Param(5, type="integer", title="行数"),
        "formats": Param("iceberg,delta,hudi,paimon", type="string", title="格式",
                         description="逗号分隔，要建的湖格式子集"),
    },
) as dag:
    submit = SparkKubernetesOperator(
        task_id="submit_four_lakes",
        namespace=JOB_NAMESPACE,
        template_spec=build_spark_application(),
        get_logs=True,
        delete_on_termination=False,
        do_xcom_push=False,
    )
