"""Spark WordCount 示例 DAG（KubernetesPodOperator，镜像/参数均可按触发覆盖）。

相比 BashOperator + executor_config.pod_override：KubernetesExecutor 的 executor_config
不会做 Jinja 模板渲染（pod_override 里的 image 会变成字面量 "{{ params.image }}"，导致
ImagePullBackOff），因此无法按「本次触发」切换镜像。KubernetesPodOperator 的 image / cmds /
arguments / env_vars 都是模板字段，可以真正用 DAG 参数 image 在每次触发时指定 worker 镜像。

参数（params）触发时可在 UI「Trigger DAG w/ config」或 REST/CLI 的 --conf 覆盖：
  - image  : 任务 Pod 镜像（需含 Spark/Java），默认 Gluten Spark 4.0.1 镜像
  - master : Spark master，如 local[*] / spark://host:7077 / k8s://https://... / yarn
  - input  : 输入（本地路径或 http(s) URL；URL 会先下载到 /tmp 再交给 Spark）
  - output : 输出目录（overwrite 覆盖）

KPO 建的任务 Pod 独立、无 gitSync，因此 Spark 应用（wordcount.py）从公开 raw URL 拉取；
默认输入也用公开 raw URL。私有 ACR 镜像由节点 kubelet 的 AcrPull 在节点级拉取，Pod 无需额外凭据。
Pod 日志经 get_logs=True 回流到任务日志（再由 worker 的远程日志写入 Blob）。
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
# 默认输入（公开 raw 文件）——URL 会先下载到本地再交给 Spark。
DEFAULT_INPUT = "https://raw.githubusercontent.com/wgqcd88/airflow-dag-demo/main/dags/hello_world.py"

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
echo "提交命令: $SPARK_SUBMIT --master $SPARK_MASTER $APP $IN $SPARK_OUTPUT"
exec "$SPARK_SUBMIT" --master "$SPARK_MASTER" "$APP" "$IN" "$SPARK_OUTPUT"
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
            description="本地路径或 http(s) URL（URL 会先下载再交给 Spark）",
        ),
        "output": Param(
            "/tmp/wordcount_output",
            type="string",
            title="输出目录",
            description="结果写出目录（overwrite）",
        ),
    },
) as dag:
    submit = KubernetesPodOperator(
        task_id="submit",
        name="spark-wordcount-kpo",
        namespace=KPO_NAMESPACE,
        image="{{ params.image }}",
        cmds=["bash", "-c", CONTAINER_SCRIPT],
        env_vars={
            "SPARK_MASTER": "{{ params.master }}",
            "SPARK_INPUT": "{{ params.input }}",
            "SPARK_OUTPUT": "{{ params.output }}",
            "SPARK_APP_URL": SPARK_APP_URL,
        },
        get_logs=True,
        in_cluster=True,
        on_finish_action="delete_pod",
        image_pull_policy="IfNotPresent",
        startup_timeout_seconds=600,
    )
