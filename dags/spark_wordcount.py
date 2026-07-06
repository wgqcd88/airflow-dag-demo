"""Spark WordCount 示例 DAG（参数化 master / input / output）。

通过 DAG 参数（params）传入 Spark 运行参数；触发时可在 UI 的
"Trigger DAG w/ config" 或 REST / CLI 的 --conf 覆盖：

  - master : Spark master，如 local[*] / spark://host:7077 / k8s://https://... / yarn
  - input  : 输入路径（文件/目录/glob，支持 file:// 或 abfss:// 等 Spark 可识别 scheme）
  - output : 输出目录（overwrite 覆盖）

任务用 spark-submit 提交同目录 spark_apps/wordcount.py。
兼容 Airflow 2.x / 3.x（TaskFlow API）。
"""

from __future__ import annotations

import os
import shlex
import subprocess

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param

# 应用脚本随本 DAG 一起由 git-sync 拉取，解析时即可确定绝对路径。
SPARK_APP = os.path.join(os.path.dirname(__file__), "spark_apps", "wordcount.py")


@dag(
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
)
def spark_wordcount():
    @task
    def submit(params: dict | None = None) -> str:
        params = params or {}
        master = params["master"]
        input_path = params["input"]
        output_path = params["output"]

        cmd = [
            "spark-submit",
            "--master", master,
            SPARK_APP,
            input_path,
            output_path,
        ]
        print("提交命令:", " ".join(shlex.quote(c) for c in cmd))
        subprocess.run(cmd, check=True)
        return output_path

    submit()


spark_wordcount()
