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
import shutil
import subprocess

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param

# 应用脚本随本 DAG 一起由 git-sync 拉取，解析时即可确定绝对路径。
SPARK_APP = os.path.join(os.path.dirname(__file__), "spark_apps", "wordcount.py")


def _spark_submit_cmd() -> list[str]:
    """解析可用的 spark-submit 可执行命令。

    某些镜像里 pip 装的 pyspark（或 COPY 进来的 Spark）其 bin/ 脚本
    丢失了执行位，直接调 `spark-submit` 会报 PermissionError: [Errno 13]。
    这里优先用 pyspark 自带的 bin/spark-submit，并尽力补上 +x 执行权限，
    最后回退到 PATH 上的 spark-submit。
    """
    try:
        import pyspark

        bindir = os.path.join(os.path.dirname(pyspark.__file__), "bin")
        if os.path.isdir(bindir):
            # 尽力给 bin/ 下脚本补执行权限（spark-submit 会内部调用 spark-class 等）
            for name in os.listdir(bindir):
                fp = os.path.join(bindir, name)
                try:
                    if os.path.isfile(fp):
                        os.chmod(fp, 0o755)
                except OSError:
                    pass
            ss = os.path.join(bindir, "spark-submit")
            if os.path.isfile(ss):
                return [ss]
    except Exception:
        pass

    found = shutil.which("spark-submit")
    if found:
        return [found]

    raise RuntimeError(
        "找不到可用的 spark-submit：请在镜像中安装 pyspark（pip install pyspark）"
        "或部署 Spark，并确保 JAVA_HOME 已配置、bin/ 脚本具备执行权限。"
    )


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

        cmd = _spark_submit_cmd() + [
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
