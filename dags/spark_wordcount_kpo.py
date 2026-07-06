"""Spark WordCount 示例 DAG（KubernetesPodOperator，镜像/参数均可按触发覆盖）。

相比 BashOperator + executor_config.pod_override：KubernetesExecutor 的 executor_config
不会做 Jinja 模板渲染（pod_override 里的 image 会变成字面量 "{{ params.image }}"，导致
ImagePullBackOff），因此无法按「本次触发」切换镜像。KubernetesPodOperator 的 image / cmds /
arguments / env_vars 都是模板字段，可以真正用 DAG 参数 image 在每次触发时指定 worker 镜像。

参数（params）触发时可在 UI「Trigger DAG w/ config」或 REST/CLI 的 --conf 覆盖：
  - image  : 任务 Pod 镜像（需含 Spark/Java），默认 Gluten Spark 4.0.1 镜像
  - master : Spark master，如 local[*] / spark://host:7077 / k8s://https://... / yarn
  - input  : 输入路径，默认 ADLS Gen2 对象存储 abfss://...（也支持本地路径 / http(s) URL）
  - output : 输出目录，默认 ADLS Gen2 对象存储 abfss://...（overwrite 覆盖）

读写对象存储：input/output 用 ADLS Gen2（abfss://<容器>@<账户>.dfs.core.windows.net/...）。
鉴权用 Azure Workload Identity——KPO Pod 以 airflow-worker SA 运行并带
azure.workload.identity/use=true 标签，webhook 注入 AZURE_CLIENT_ID/TENANT_ID/联合令牌文件；
容器脚本据此拼 ABFS OAuth（WorkloadIdentityTokenProvider）spark 配置。账户共享密钥已禁用，仅走 AAD。

KPO 建的任务 Pod 独立、无 gitSync，因此 Spark 应用（wordcount.py）从公开 raw URL 拉取。
私有 ACR 镜像由节点 kubelet 的 AcrPull 在节点级拉取。Pod 日志经 get_logs=True 回流到任务日志。
"""

from __future__ import annotations

import os

import pendulum
from airflow.models.dag import DAG
from airflow.models.param import Param

try:  # Airflow 3.x / 新 provider
    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
except ImportError:  # 旧 provider 兼容
    from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
        KubernetesPodOperator,
    )

# 默认 worker 镜像（含 Spark 4.0.1 + Gluten/Velox）。可用环境变量或触发参数覆盖。
WORKER_IMAGE = os.getenv(
    "SPARK_WORKER_IMAGE",
    "acrsparktpcdstestcca.azurecr.io/airflow-spark:4.0.1-gluten",
)
# KPO 任务 Pod 所在命名空间（worker SA 已有在此建 Pod 的 RBAC）。
KPO_NAMESPACE = os.getenv("SPARK_KPO_NAMESPACE", "airflow")
# Spark 应用（wordcount.py）公开 raw 地址——KPO Pod 无 gitSync，运行时下载。
SPARK_APP_URL = os.getenv(
    "SPARK_APP_URL",
    "https://raw.githubusercontent.com/wgqcd88/airflow-dag-demo/main/dags/spark_apps/wordcount.py",
)
# ADLS Gen2 账户 host（用于 ABFS OAuth 配置键）。为空则不注入 ABFS 配置（如纯本地/URL 输入）。
ADLS_HOST = os.getenv("SPARK_ADLS_HOST", "sasparktpcdstestcca.dfs.core.windows.net")
# 默认读写对象存储（ADLS Gen2 / abfss），用 Workload Identity 鉴权。
DEFAULT_INPUT = os.getenv(
    "SPARK_DEFAULT_INPUT",
    "abfss://spark-data@sasparktpcdstestcca.dfs.core.windows.net/wordcount/input/sample.txt",
)
DEFAULT_OUTPUT = os.getenv(
    "SPARK_DEFAULT_OUTPUT",
    "abfss://spark-data@sasparktpcdstestcca.dfs.core.windows.net/wordcount/output",
)
# KPO 任务 Pod 使用的 SA（带 azure.workload.identity/client-id 注解）。
WORKER_SA = os.getenv("SPARK_WORKER_SA", "airflow-worker")

# 容器内脚本：定位 spark-submit、下载 app（必要时下载 URL 输入）、提交。
CONTAINER_SCRIPT = r"""
set -euo pipefail
SPARK_SUBMIT="$(command -v spark-submit || true)"
if [ -z "$SPARK_SUBMIT" ] && [ -n "${SPARK_HOME:-}" ] && [ -x "$SPARK_HOME/bin/spark-submit" ]; then
  SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"
fi
if [ -z "$SPARK_SUBMIT" ]; then
  PYSPARK_HOME="$(python3 -c 'import pyspark,os;print(os.path.dirname(pyspark.__file__))' 2>/dev/null || true)"
  if [ -n "$PYSPARK_HOME" ] && [ -f "$PYSPARK_HOME/bin/spark-submit" ]; then
    chmod +x "$PYSPARK_HOME"/bin/* 2>/dev/null || true
    SPARK_SUBMIT="$PYSPARK_HOME/bin/spark-submit"
  fi
fi
if [ -z "$SPARK_SUBMIT" ]; then SPARK_SUBMIT="${SPARK_HOME:-/opt/spark}/bin/spark-submit"; fi
fetch() { python3 -c "import sys,urllib.request; urllib.request.urlretrieve(sys.argv[1], sys.argv[2])" "$1" "$2"; }
APP=/tmp/wordcount.py
fetch "$SPARK_APP_URL" "$APP"
IN="$SPARK_INPUT"
case "$IN" in
  http://*|https://*) fetch "$IN" /tmp/spark_input_data; IN=/tmp/spark_input_data ;;
esac
# 若目标是 ADLS Gen2（abfss）且注入了 Workload Identity 环境变量，则拼 ABFS OAuth 配置。
CONF_ARGS=()
if [ -n "${SPARK_ADLS_HOST:-}" ] && [ -n "${AZURE_CLIENT_ID:-}" ] && [ -n "${AZURE_FEDERATED_TOKEN_FILE:-}" ]; then
  H="$SPARK_ADLS_HOST"
  CONF_ARGS+=(--conf "spark.hadoop.fs.azure.account.auth.type.$H=OAuth")
  CONF_ARGS+=(--conf "spark.hadoop.fs.azure.account.oauth.provider.type.$H=org.apache.hadoop.fs.azurebfs.oauth2.WorkloadIdentityTokenProvider")
  CONF_ARGS+=(--conf "spark.hadoop.fs.azure.account.oauth2.client.id.$H=$AZURE_CLIENT_ID")
  CONF_ARGS+=(--conf "spark.hadoop.fs.azure.account.oauth2.client.endpoint.$H=https://login.microsoftonline.com/${AZURE_TENANT_ID}/oauth2/token")
  CONF_ARGS+=(--conf "spark.hadoop.fs.azure.account.oauth2.token.file.$H=$AZURE_FEDERATED_TOKEN_FILE")
  echo "已启用 ABFS Workload Identity（account=$H client=$AZURE_CLIENT_ID）"
fi
echo "提交命令: $SPARK_SUBMIT --master $SPARK_MASTER [ABFS conf x$((${#CONF_ARGS[@]}/2))] $APP $IN $SPARK_OUTPUT"
exec "$SPARK_SUBMIT" --master "$SPARK_MASTER" ${CONF_ARGS[@]+"${CONF_ARGS[@]}"} "$APP" "$IN" "$SPARK_OUTPUT"
"""

with DAG(
    dag_id="spark_wordcount_kpo",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["demo", "spark", "kpo"],
    params={
        "image": Param(
            WORKER_IMAGE,
            type="string",
            title="Worker 镜像",
            description="任务 Pod 镜像（需含 Spark/Java）；每次触发可覆盖",
        ),
        "master": Param(
            "local[*]",
            type="string",
            title="Spark master",
            description="local[*] / spark://host:7077 / k8s://https://... / yarn",
        ),
        "input": Param(
            DEFAULT_INPUT,
            type="string",
            title="输入路径",
            description="ADLS Gen2 abfss:// 对象存储（默认）/ 本地路径 / http(s) URL",
        ),
        "output": Param(
            DEFAULT_OUTPUT,
            type="string",
            title="输出目录",
            description="ADLS Gen2 abfss:// 对象存储（默认，overwrite 覆盖）",
        ),
    },
) as dag:
    submit = KubernetesPodOperator(
        task_id="submit",
        name="spark-wordcount-kpo",
        namespace=KPO_NAMESPACE,
        service_account_name=WORKER_SA,
        labels={"azure.workload.identity/use": "true"},
        image="{{ params.image }}",
        cmds=["bash", "-c", CONTAINER_SCRIPT],
        env_vars={
            "SPARK_MASTER": "{{ params.master }}",
            "SPARK_INPUT": "{{ params.input }}",
            "SPARK_OUTPUT": "{{ params.output }}",
            "SPARK_APP_URL": SPARK_APP_URL,
            "SPARK_ADLS_HOST": ADLS_HOST,
        },
        get_logs=True,
        in_cluster=True,
        on_finish_action="delete_pod",
        image_pull_policy="IfNotPresent",
        startup_timeout_seconds=600,
    )
