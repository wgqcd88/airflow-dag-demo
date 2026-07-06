"""Spark WordCount 示例 DAG（BashOperator 执行 spark-submit，参数化 master/input/output）。

参数（params）触发时可在 UI「Trigger DAG w/ config」或 REST/CLI 的 --conf 覆盖：
  - master : Spark master，如 local[*] / spark://host:7077 / k8s://https://... / yarn
  - input  : 输入路径（Spark 可读的文件/目录/glob）
  - output : 输出目录（overwrite 覆盖）

用 BashOperator 调 spark-submit 提交同目录 spark_apps/wordcount.py。
参数经 env 注入（Jinja 模板化），append_env=True 以保留镜像的 PATH/JAVA_HOME/SPARK_HOME。
兼容 Airflow 2.x / 3.x。
"""

from __future__ import annotations

import os

import pendulum
from airflow.models.dag import DAG
from airflow.models.param import Param

try:  # Airflow 3.x
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:  # Airflow 2.x
    from airflow.operators.bash import BashOperator

from kubernetes.client import models as k8s

# 应用脚本随本 DAG 由 git-sync 一起拉取，解析时即可确定绝对路径。
SPARK_APP = os.path.join(os.path.dirname(__file__), "spark_apps", "wordcount.py")

# KubernetesExecutor：用 executor_config.pod_override 给「本任务」单独指定 worker 镜像
# （含 Spark/Java），基础 airflow 组件可继续用精简镜像。可用环境变量覆盖默认值。
WORKER_IMAGE = os.getenv(
    "SPARK_WORKER_IMAGE",
    "acrsparktpcdstestcca.azurecr.io/airflow-spark:4.0.1-gluten",
)

# 找 PATH 上的 spark-submit，找不到回退 $SPARK_HOME/bin/spark-submit；用双引号安全传 local[*] 与路径。
BASH_CMD = r"""
set -euo pipefail
SPARK_SUBMIT="$(command -v spark-submit || true)"
if [ -z "$SPARK_SUBMIT" ]; then SPARK_SUBMIT="${SPARK_HOME:-/opt/spark}/bin/spark-submit"; fi
echo "提交命令: $SPARK_SUBMIT --master $SPARK_MASTER $SPARK_APP $SPARK_INPUT $SPARK_OUTPUT"
exec "$SPARK_SUBMIT" --master "$SPARK_MASTER" "$SPARK_APP" "$SPARK_INPUT" "$SPARK_OUTPUT"
"""

with DAG(
    dag_id="spark_wordcount",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["demo", "spark"],
    params={
        "master": Param(
            "local[*]",
            type="string",
            title="Spark master",
            description="local[*] / spark://host:7077 / k8s://https://... / yarn",
        ),
        "input": Param(
            "/opt/airflow/dags/repo/dags/hello_world.py",
            type="string",
            title="输入路径",
            description="Spark 可读的文件/目录/glob",
        ),
        "output": Param(
            "/tmp/wordcount_output",
            type="string",
            title="输出目录",
            description="结果写出目录（overwrite）",
        ),
    },
) as dag:
    submit = BashOperator(
        task_id="submit",
        bash_command=BASH_CMD,
        env={
            "SPARK_MASTER": "{{ params.master }}",
            "SPARK_APP": SPARK_APP,
            "SPARK_INPUT": "{{ params.input }}",
            "SPARK_OUTPUT": "{{ params.output }}",
        },
        append_env=True,
        # KubernetesExecutor：用 pod_override 覆盖「本任务」Pod 的镜像。
        # 容器名必须为 "base"（与任务容器同名才会合并覆盖）。
        # 注意：executor_config 不做 Jinja 模板渲染，故镜像只能用解析期常量
        # （不能 {{ params.image }}）。要「按触发切换镜像」请用 spark_wordcount_kpo（KPO）。
        executor_config={
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[k8s.V1Container(name="base", image=WORKER_IMAGE)]
                )
            )
        },
    )
