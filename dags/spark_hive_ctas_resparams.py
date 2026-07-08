"""示例 DAG：演示 spark_resource_params 通用资源参数模板。

复用已验证的 PySpark 应用 hive_table_demo.py（连 HMS 建表落 ADLS），但把
driver/executor 的 CPU、内存、executor 实例数做成 DAG 里单独可见、可覆盖的 Param
（来自 spark_resource_params 模块），而非写死的模块常量。

资源参数如何进入 SparkApplication CRD（避开 integer 字段无法走 Jinja 的限制）见
spark_resource_params.py 的模块说明：CPU 走 coreRequest/coreLimit(string)，
实例数走 spark.executor.instances(sparkConf)。
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
IMAGE = os.getenv(
    "SPARK_PYSPARK_IMAGE",
    "ghcr.io/wgqcd88/spark:3.5.4-gluten-pyspark-20260707",
)
DEFAULT_APP_FILE_URL = os.getenv(
    "SPARK_APP_FILE_URL",
    "https://raw.githubusercontent.com/wgqcd88/airflow-dag-demo/main/dags/spark_apps/hive_table_demo.py",
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
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": EVENTLOG_DIR,
        "spark.plugins": "org.apache.gluten.GlutenPlugin",
        "spark.memory.offHeap.enabled": "true",
        "spark.memory.offHeap.size": "1g",
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
            "metadata": {"name": "hive-ctas-resparams", "namespace": JOB_NAMESPACE},
            "spec": {
                "type": "Python",
                "pythonVersion": "3",
                "mode": "cluster",
                "image": IMAGE,
                "imagePullPolicy": "IfNotPresent",
                "mainApplicationFile": "{{ params.app_file_url }}",
                "arguments": ["--table", "{{ params.table }}", "--rows", "{{ params.rows }}"],
                "sparkVersion": "3.5.4",
                "sparkConf": spark_conf,
                "restartPolicy": {"type": "Never"},
                "driver": driver_spec(service_account=SERVICE_ACCOUNT, labels=labels),
                "executor": executor_spec(labels=labels),
            },
        }
    }


with DAG(
    dag_id="spark_hive_ctas_resparams",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["demo", "spark", "spark-operator", "resource-params"],
    params={
        # 业务参数
        "table": Param("demo_db.res_demo", type="string", title="目标表"),
        "rows": Param(5, type="integer", title="行数"),
        "app_file_url": Param(DEFAULT_APP_FILE_URL, type="string", title="PySpark 主文件 URL"),
        # 资源参数（单独显示，来自通用模板）
        **RESOURCE_PARAMS,
    },
) as dag:
    submit = SparkKubernetesOperator(
        task_id="submit",
        namespace=JOB_NAMESPACE,
        template_spec=build_spark_application(),
        get_logs=True,
        delete_on_termination=False,
        do_xcom_push=False,
    )
