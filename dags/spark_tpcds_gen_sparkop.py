"""通过 SparkKubernetesOperator（Spark Operator / SparkApplication CRD）调度 PySpark 作业：
用 Kyuubi TPC-DS connector 生成数据，CTAS 成 Hive Metastore 实体表（parquet，落 ADLS）。

Kyuubi TPC-DS connector（org.apache.kyuubi:kyuubi-spark-connector-tpcds）通过
spark.jars.packages 运行时从 Maven 拉取（镜像无此 jar），注册为 catalog `tpcds`。
其 database 按 scale 命名 sf<scale>；本作业把该 scale 下的表 CTAS 成实体表。

镜像用带 python 的 `4.0.1-gluten-pyspark-lakerss`（Spark 4.0 / Scala 2.13），
故 connector 用 2.13 版。元数据走 HMS，数据落 ADLS warehouse。

参数（触发时可覆盖）：
    - scale        TPC-DS scale factor（默认 1），对应 tpcds.sf<scale>
    - db           目标实体表 database（默认空 -> 应用侧用 tpcds_sf<scale>）
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

# 带 python 的 Spark 4.0 lakerss 镜像（Scala 2.13）。
IMAGE = os.getenv(
    "SPARK_TPCDS_IMAGE",
    "ghcr.io/wgqcd88/spark:3.5.4-gluten-pyspark-lakerss-20260712",
)
# Kyuubi TPC-DS connector Maven 坐标（Scala 2.13 配 Spark 4.0）。
KYUUBI_TPCDS_PACKAGE = os.getenv(
    "KYUUBI_TPCDS_PACKAGE",
    "org.apache.kyuubi:kyuubi-spark-connector-tpcds_2.12:1.11.1",
)

DEFAULT_APP_FILE_URL = os.getenv(
    "SPARK_APP_FILE_URL",
    "https://raw.githubusercontent.com/wgqcd88/airflow-dag-demo/main/dags/spark_apps/tpcds_gen.py",
)

HMS_URIS = os.getenv(
    "HIVE_METASTORE_URIS",
    "thrift://hive-metastore.data-platform.svc.cluster.local:9083",
)
ADLS_HOST = os.getenv("SPARK_ADLS_HOST", "wgqjesa.dfs.core.windows.net")
WAREHOUSE_DIR = os.getenv(
    "SPARK_WAREHOUSE_DIR", "abfss://warehouse@wgqjesa.dfs.core.windows.net/"
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

# driver/executor 资源改由 spark_resource_params 通用模板提供（可在触发时覆盖），
# 见 params={**RESOURCE_PARAMS} 与 driver_spec/executor_spec/resource_spark_conf。


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
        # Spark Operator 的 pod HOME=/nonexistent 不可写，Ivy 默认缓存 $HOME/.ivy2 会报
        # FileNotFoundException；显式指到可写的 /tmp。
        "spark.jars.ivy": "/tmp/.ivy2",
        "spark.sql.catalog.tpcds": "org.apache.kyuubi.spark.connector.tpcds.TPCDSCatalog",
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
    spark_conf.update(resource_spark_conf())  # spark.executor.instances = {{ params.executor_instances }}

    labels = {"azure.workload.identity/use": "true"}
    return {
        "spark": {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "metadata": {"name": "tpcds-gen", "namespace": JOB_NAMESPACE},
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
                "sparkVersion": "4.0.1",
                "sparkConf": spark_conf,
                "restartPolicy": {"type": "Never"},
                "driver": driver_spec(service_account=SERVICE_ACCOUNT, labels=labels),
                "executor": executor_spec(labels=labels),
            },
        }
    }


with DAG(
    dag_id="spark_tpcds_gen_sparkop",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["demo", "spark", "spark-operator", "tpcds", "kyuubi"],
    params={
        "scale": Param("1", type="string", title="Scale factor",
                       description="TPC-DS scale，对应 tpcds.sf<scale>（如 1 / 10）"),
        "db": Param("", type="string", title="目标 database",
                    description="留空则用 tpcds_sf<scale>"),
        "tables": Param("all", type="string", title="表",
                        description="all 或逗号分隔表名子集，如 store_sales,item,customer"),
        "app_file_url": Param(
            DEFAULT_APP_FILE_URL, type="string", title="PySpark 主文件 URL",
            description="默认 main 分支 raw URL；raw CDN 有缓存，紧急用 commit-SHA 路径。",
        ),
        # 资源参数（单独显示，来自通用模板）。TPC-DS 数据量较大，覆盖模板默认为更大的默认值；
        # 触发时仍可按 scale 调整（如 sf10 调大 executor 内存/实例数）。
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
        task_id="submit_tpcds_gen",
        namespace=JOB_NAMESPACE,
        template_spec=build_spark_application(),
        get_logs=True,
        delete_on_termination=False,
        do_xcom_push=False,
    )
