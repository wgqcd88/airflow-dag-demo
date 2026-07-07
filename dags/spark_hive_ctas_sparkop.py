"""通过 SparkKubernetesOperator（Spark Operator / SparkApplication CRD）调度 PySpark 作业：
连 Hive Metastore 建表，数据落 ADLS Gen2（Storage Account）。

与 spark_wordcount_kpo.py（KubernetesPodOperator + spark-submit）并存、互为对照：
    - KPO 方式：Airflow 起一个 pod 在里面手动 spark-submit（命令式）。
    - 本 DAG：Airflow 向集群提交一个 SparkApplication CRD，由 Kubeflow Spark Operator
      接管、创建并管理 driver/executor（声明式）。SparkApplication 的状态、driver/executor
      的生命周期都归 operator 管，可用 `kubectl get sparkapplication` 观察。

前置依赖（已在集群就绪）：
    - Kubeflow Spark Operator（ns spark-operator，watch jobNamespaces=data-platform）。
    - Hive Metastore：thrift://hive-metastore.data-platform.svc.cluster.local:9083，
      warehouse.dir=abfss://warehouse@wgqjesa.dfs.core.windows.net/。
    - ServiceAccount spark-sa（含 WI 注解 client-id 8b8e1707-...，及 spark-driver-role）。
    - 带 python 的镜像：ghcr.io/wgqcd88/spark:<ver>-gluten-pyspark-20260707。

鉴权：driver/executor 都带 azure.workload.identity/use=true label，WI webhook 注入
AZURE_* 与联合令牌文件；sparkConf 里的 ABFS OAuth 配置键据此对 wgqjesa 账户走 AAD。

参数（触发时可覆盖）：
    - image        : 作业镜像（3.4.4 / 3.5.4 / 4.0.1 gluten-pyspark）
    - app_file_url : PySpark 主文件地址（默认 main 分支 raw URL；注意 GitHub raw 对 mutable
                     分支路径有约 5 分钟 CDN 缓存，刚 push 后立即触发可能拉旧版——紧急时改用
                     不可变的 commit-SHA raw 路径）
    - table        : 目标表 database.table
    - rows         : 造多少行示例数据
    - executor_instances / driver/executor 资源
"""

from __future__ import annotations

import os

import pendulum

try:  # Airflow 3.x 推荐路径
    from airflow.sdk import DAG, Param
except ImportError:  # 兼容旧版
    from airflow.models.dag import DAG
    from airflow.models.param import Param

try:  # 新 provider 路径
    from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
        SparkKubernetesOperator,
    )
except ImportError:  # 兼容
    from airflow.providers.cncf.kubernetes.operators.spark_kubernetes_operator import (
        SparkKubernetesOperator,
    )

# 作业与 operator 所在命名空间（operator watch data-platform）。
JOB_NAMESPACE = os.getenv("SPARK_JOB_NAMESPACE", "data-platform")

# 带 python 的 gluten-pyspark 镜像（PySpark 需 python 解释器，gluten 基础镜像无 python）。
IMAGE_CHOICES = (
    "ghcr.io/wgqcd88/spark:3.4.4-gluten-pyspark-20260707",
    "ghcr.io/wgqcd88/spark:3.5.4-gluten-pyspark-20260707",
    "ghcr.io/wgqcd88/spark:4.0.1-gluten-pyspark-20260707",
)
DEFAULT_IMAGE = os.getenv("SPARK_PYSPARK_IMAGE", IMAGE_CHOICES[1])  # 默认 3.5.4

# PySpark 主文件公开 raw 地址（Spark Operator 支持 remote http mainApplicationFile）。
DEFAULT_APP_FILE_URL = os.getenv(
    "SPARK_APP_FILE_URL",
    "https://raw.githubusercontent.com/wgqcd88/airflow-dag-demo/main/dags/spark_apps/hive_table_demo.py",
)

# Hive Metastore thrift 地址（Japan East 这套 HMS 在 data-platform ns）。
HMS_URIS = os.getenv(
    "HIVE_METASTORE_URIS",
    "thrift://hive-metastore.data-platform.svc.cluster.local:9083",
)
# ADLS Gen2 账户 host 及 warehouse（与 HMS hive-site.xml 一致）。
ADLS_HOST = os.getenv("SPARK_ADLS_HOST", "wgqjesa.dfs.core.windows.net")
WAREHOUSE_DIR = os.getenv(
    "SPARK_WAREHOUSE_DIR", "abfss://warehouse@wgqjesa.dfs.core.windows.net/"
)
# WI 的 tenant / client-id（与 spark-sa 注解、HMS hive-site.xml 配置一致）。
# 注意：WorkloadIdentityTokenProvider 需显式配 client.id，不会自动读 AZURE_CLIENT_ID 环境变量。
AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID", "16b3c013-d300-468d-ac64-7eda0820b6d3")
AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID", "8b8e1707-3dd1-4942-a0f6-9f5038b5c74e")
# WI 联合令牌文件路径（webhook 注入到 pod 的标准路径）。
AZURE_FEDERATED_TOKEN_FILE = os.getenv(
    "AZURE_FEDERATED_TOKEN_FILE", "/var/run/secrets/azure/tokens/azure-identity-token"
)

SERVICE_ACCOUNT = os.getenv("SPARK_SERVICE_ACCOUNT", "spark-sa")

# 资源数值字段（cores/instances）在 SparkApplication CRD 里必须是 integer 类型，
# 不能是 Jinja 渲染出的字符串，故用模块级常量（DAG 解析期即为真 int），不走 params 模板。
# 需要调整时改这些 env / 默认值后重触发即可；image/app_file_url/table/rows 等字符串字段仍走 params。
DRIVER_CORES = int(os.getenv("SPARK_DRIVER_CORES", "1"))
DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "1g")
EXECUTOR_CORES = int(os.getenv("SPARK_EXECUTOR_CORES", "1"))
EXECUTOR_INSTANCES = int(os.getenv("SPARK_EXECUTOR_INSTANCES", "1"))
EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "2g")


def _abfs_oauth_conf(host: str) -> dict:
    """ADLS Gen2 abfss 走 Workload Identity 的 OAuth 配置键（account 精确匹配 host）。"""
    # 与 HMS hive-site.xml 完全一致的 5 个键：auth.type / provider.type / tenant / client.id / token.file。
    # WorkloadIdentityTokenProvider 需显式配 client.id 与 token.file，缺 client.id 会报
    # "Configuration property fs.azure.account.oauth2.client.id not found"。
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
    """构造 SparkApplication CRD 的 template_spec（顶层需包一层 {"spark": ...}）。

    字符串字段（image / mainApplicationFile / arguments）用 Jinja 模板占位，由
    SparkKubernetesOperator 在运行时对 template_spec 做模板渲染填入 params；
    数值字段（cores/instances）用模块级 int 常量（CRD 要求 integer 类型）。
    """
    spark_conf = {
        "spark.hadoop.hive.metastore.uris": HMS_URIS,
        "spark.sql.warehouse.dir": WAREHOUSE_DIR,
        # Gluten/Velox 插件（镜像已内置 jar）；如需纯 vanilla 可去掉这两行。
        "spark.plugins": "org.apache.gluten.GlutenPlugin",
        "spark.memory.offHeap.enabled": "true",
        "spark.memory.offHeap.size": "1g",
    }
    spark_conf.update(_abfs_oauth_conf(ADLS_HOST))

    labels = {"azure.workload.identity/use": "true"}

    return {
        "spark": {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "metadata": {
                # 名字由 operator 加随机后缀（random_name_suffix=True）。
                "name": "hive-table-demo",
                "namespace": JOB_NAMESPACE,
            },
            "spec": {
                "type": "Python",
                "pythonVersion": "3",
                "mode": "cluster",
                "image": "{{ params.image }}",
                "imagePullPolicy": "IfNotPresent",
                "mainApplicationFile": "{{ params.app_file_url }}",
                "arguments": [
                    "--table", "{{ params.table }}",
                    "--rows", "{{ params.rows }}",
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
    dag_id="spark_hive_ctas_sparkop",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["demo", "spark", "spark-operator", "hive", "adls"],
    params={
        "image": Param(
            DEFAULT_IMAGE, type="string", title="镜像",
            description="带 python 的 gluten-pyspark 镜像",
            enum=list(IMAGE_CHOICES),
        ),
        "app_file_url": Param(
            DEFAULT_APP_FILE_URL, type="string", title="PySpark 主文件 URL",
            description=(
                "默认 main 分支 raw URL。GitHub raw 对 mutable 分支路径有约 5 分钟 CDN 缓存，"
                "刚 push 后立即触发可能拉旧版；紧急时改用 commit-SHA 不可变路径。"
            ),
        ),
        "table": Param("demo_db.airflow_demo", type="string", title="目标表",
                       description="database.table"),
        "rows": Param(5, type="integer", title="行数", description="造多少行示例数据"),
        # 资源数值（cores/instances）走模块级常量而非 param——CRD 要求 integer 类型，
        # 而 Jinja 渲染只能产出字符串，故不暴露为可覆盖 param（改 env/默认值后重触发即可）。
    },
) as dag:
    submit = SparkKubernetesOperator(
        task_id="submit_sparkapplication",
        namespace=JOB_NAMESPACE,
        # template_spec 里的字符串占位（image/mainApplicationFile/arguments）由 operator
        # 在运行时按 params 做 Jinja 渲染；数值字段已是 int 常量。
        template_spec=build_spark_application(),
        get_logs=True,
        delete_on_termination=False,  # 保留 SparkApplication 便于查状态/日志
        do_xcom_push=False,
    )
