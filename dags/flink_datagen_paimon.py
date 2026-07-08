"""通过 Flink Kubernetes Operator（FlinkDeployment CRD）执行 PyFlink 流任务：
datagen 源 -> Paimon 表（Hive catalog，元数据 HMS，数据落 ADLS Gen2）。

Airflow 无 apache-flink provider，故用 cncf-kubernetes 的通用
KubernetesCreateResourceOperator 提交 FlinkDeployment CRD（Application 模式），
用 KubernetesDeleteResourceOperator 在需要时清理。

PyFlink 作业 datagen_to_paimon.py（无镜像内置）由 initContainer 从仓库 raw URL 拉到
共享 emptyDir，jobManager 以 Application 模式经 PythonDriver 执行它。

依赖：
    - 集群已装 Flink Kubernetes Operator（watch 所有 ns，识别 FlinkDeployment CRD）
    - 镜像 ghcr.io/wgqcd88/flink:1.20.4-pyflink-hive-datalake-20260708
      （Flink 1.20.4 + python3.10 + paimon-flink + hive connector + azure-fs-hadoop 插件）
    - HMS: thrift://hive-metastore.data-platform.svc.cluster.local:9083
    - ADLS Workload Identity（spark-sa 的 WI，Pod 带 azure.workload.identity/use=true）
    - airflow-sa 需有创建 flinkdeployments 的 RBAC

参数（触发可覆盖）：
    - app_file_url : PyFlink 主文件 raw URL（raw CDN 缓存时用 commit-SHA）
    - paimon_db / paimon_table / rows_per_sec
"""

from __future__ import annotations

import os

import pendulum

try:
    from airflow.sdk import DAG, Param
except ImportError:
    from airflow.models.dag import DAG
    from airflow.models.param import Param

from airflow.providers.cncf.kubernetes.operators.resource import (
    KubernetesCreateResourceOperator,
    KubernetesDeleteResourceOperator,
)

JOB_NAMESPACE = os.getenv("SPARK_JOB_NAMESPACE", "data-platform")
IMAGE = os.getenv(
    "FLINK_IMAGE", "ghcr.io/wgqcd88/flink:1.20.4-pyflink-hive-datalake-20260708"
)
DEFAULT_APP_FILE_URL = os.getenv(
    "FLINK_APP_FILE_URL",
    "https://raw.githubusercontent.com/wgqcd88/airflow-dag-demo/main/dags/flink_apps/datagen_to_paimon.py",
)
HMS_URIS = os.getenv(
    "HIVE_METASTORE_URIS",
    "thrift://hive-metastore.data-platform.svc.cluster.local:9083",
)
ADLS_HOST = os.getenv("FLINK_ADLS_HOST", "wgqjesa.dfs.core.windows.net")
PAIMON_WAREHOUSE = os.getenv(
    "PAIMON_WAREHOUSE", "abfss://warehouse@wgqjesa.dfs.core.windows.net/paimon"
)
AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID", "16b3c013-d300-468d-ac64-7eda0820b6d3")
AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID", "8b8e1707-3dd1-4942-a0f6-9f5038b5c74e")
AZURE_FEDERATED_TOKEN_FILE = os.getenv(
    "AZURE_FEDERATED_TOKEN_FILE", "/var/run/secrets/azure/tokens/azure-identity-token"
)
SERVICE_ACCOUNT = os.getenv("FLINK_SERVICE_ACCOUNT", "spark-sa")

DEPLOY_NAME = "datagen-paimon"

# FlinkDeployment CRD YAML（Application 模式跑 PyFlink）。app_file_url/db/table 走 Jinja。
FLINKDEP_YAML = f"""
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: {DEPLOY_NAME}
  namespace: {JOB_NAMESPACE}
spec:
  image: {IMAGE}
  flinkVersion: v1_20
  serviceAccount: {SERVICE_ACCOUNT}
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "2"
    state.backend.type: rocksdb
    execution.checkpointing.interval: "30s"
    # ADLS Gen2 via Workload Identity（azure-fs-hadoop 插件读这些 fs.azure.* 配置）
    fs.azure.account.auth.type.{ADLS_HOST}: OAuth
    fs.azure.account.oauth.provider.type.{ADLS_HOST}: org.apache.hadoop.fs.azurebfs.oauth2.WorkloadIdentityTokenProvider
    fs.azure.account.oauth2.msi.tenant.{ADLS_HOST}: "{AZURE_TENANT_ID}"
    fs.azure.account.oauth2.client.id.{ADLS_HOST}: "{AZURE_CLIENT_ID}"
    fs.azure.account.oauth2.token.file.{ADLS_HOST}: "{AZURE_FEDERATED_TOKEN_FILE}"
    fs.azure.account.hns.enabled.{ADLS_HOST}: "true"
  podTemplate:
    metadata:
      labels:
        azure.workload.identity/use: "true"
    spec:
      serviceAccountName: {SERVICE_ACCOUNT}
      containers:
        - name: flink-main-container
          env:
            - name: HIVE_METASTORE_URIS
              value: "{HMS_URIS}"
            - name: PAIMON_WAREHOUSE
              value: "{PAIMON_WAREHOUSE}"
            - name: PAIMON_DB
              value: "{{{{ params.paimon_db }}}}"
            - name: PAIMON_TABLE
              value: "{{{{ params.paimon_table }}}}"
            - name: GEN_ROWS_PER_SEC
              value: "{{{{ params.rows_per_sec }}}}"
          volumeMounts:
            - name: app-vol
              mountPath: /opt/flink/usrapp
      initContainers:
        - name: fetch-app
          image: {IMAGE}
          command:
            - sh
            - -c
            - "curl -fsSL -o /opt/flink/usrapp/datagen_to_paimon.py '{{{{ params.app_file_url }}}}'"
          volumeMounts:
            - name: app-vol
              mountPath: /opt/flink/usrapp
      volumes:
        - name: app-vol
          emptyDir: {{}}
  jobManager:
    resource:
      cpu: 1
      memory: "2048m"
  taskManager:
    resource:
      cpu: 1
      memory: "2048m"
  job:
    jarURI: local:///opt/flink/opt/flink-python-1.20.4.jar
    entryClass: org.apache.flink.client.python.PythonDriver
    args:
      - "-pyclientexec"
      - "/usr/bin/python3"
      - "-py"
      - "/opt/flink/usrapp/datagen_to_paimon.py"
    parallelism: 2
    upgradeMode: stateless
    state: running
"""

with DAG(
    dag_id="flink_datagen_paimon",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["demo", "flink", "flink-operator", "paimon", "streaming"],
    params={
        "app_file_url": Param(DEFAULT_APP_FILE_URL, type="string", title="PyFlink 主文件 URL",
                              description="默认 main 分支 raw；raw CDN 缓存时用 commit-SHA"),
        "paimon_db": Param("flink_demo", type="string", title="Paimon database"),
        "paimon_table": Param("orders_stream", type="string", title="Paimon 表"),
        "rows_per_sec": Param("5", type="string", title="datagen 速率(行/秒)"),
    },
) as dag:
    submit = KubernetesCreateResourceOperator(
        task_id="submit_flinkdeployment",
        yaml_conf=FLINKDEP_YAML,
        namespace=JOB_NAMESPACE,
        custom_resource_definition=True,
    )
