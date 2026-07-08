"""SparkApplication CRD 资源参数的通用模板：把 driver/executor 的 CPU、内存、
executor 实例数等常用资源参数做成 Airflow DAG 可覆盖的 Param，供 SparkKubernetesOperator
的 template_spec 复用。

## 为什么需要这个模块

SparkApplication CRD 的 `driver.cores` / `executor.cores` / `executor.instances` 是
**integer** 类型，而 Airflow 的 Jinja 模板（`{{ params.x }}`）渲染出的永远是**字符串**，
直接塞进这些字段会被 CRD 的 schema 校验拒绝（type mismatch）。

本模块用「字符串旁路」规避：
- **CPU**：用 `coreRequest` / `coreLimit`（CRD 中是 **string** 类型，支持 "1" / "500m" / "2"），
  不用 integer 的 `cores`。这样 CPU 可以走 Jinja param。
- **内存 / memoryOverhead**：CRD 中本就是 string，直接走 param。
- **executor 实例数**：CRD 的 `executor.instances` 是 integer 走不了 Jinja，改用 Spark 配置
  `spark.executor.instances`（sparkConf 里是字符串）替代，效果等价。

## 用法

    from spark_resource_params import RESOURCE_PARAMS, driver_spec, executor_spec, resource_spark_conf

    with DAG(..., params={**RESOURCE_PARAMS, "your_other_param": Param(...)}) as dag:
        spec = {
            "spec": {
                ...
                "sparkConf": {**your_conf, **resource_spark_conf()},
                "driver": driver_spec(service_account="spark-sa", labels={...}),
                "executor": executor_spec(labels={...}),
            }
        }

driver_spec / executor_spec 返回的 dict 里，CPU/内存字段是 Jinja 占位串，由
SparkKubernetesOperator 在运行时按 params 渲染。
"""

from __future__ import annotations

try:
    from airflow.sdk import Param
except ImportError:
    from airflow.models.param import Param


# 触发 DAG 时可在 UI / --conf 覆盖的资源参数（单独显示）。
RESOURCE_PARAMS = {
    "driver_cpu": Param(
        "1", type="string", title="Driver CPU",
        description="driver 的 CPU request/limit，如 500m / 1 / 2（对应 CRD driver.coreRequest+coreLimit）",
    ),
    "driver_memory": Param(
        "1g", type="string", title="Driver 内存",
        description="driver 内存，如 1g / 2g / 4g（CRD driver.memory）",
    ),
    "executor_cpu": Param(
        "1", type="string", title="Executor CPU",
        description="每个 executor 的 CPU request/limit，如 500m / 1 / 2",
    ),
    "executor_memory": Param(
        "2g", type="string", title="Executor 内存",
        description="每个 executor 内存，如 2g / 4g / 8g",
    ),
    "executor_instances": Param(
        "2", type="string", title="Executor 实例数",
        description="executor 数量（经 spark.executor.instances 生效，非 CRD executor.instances）",
    ),
}


def resource_spark_conf() -> dict:
    """需并入 SparkApplication 的 sparkConf 的资源相关配置。

    executor.instances 是 CRD integer 字段，走不了 Jinja，故用 spark.executor.instances
    （字符串）替代实例数控制。
    """
    return {
        "spark.executor.instances": "{{ params.executor_instances }}",
    }


def driver_spec(service_account: str, labels: dict | None = None,
                extra: dict | None = None) -> dict:
    """构造 SparkApplication 的 driver 段。CPU 走 coreRequest/coreLimit（string），
    内存走 memory（string）——均由 params 渲染。"""
    spec = {
        "coreRequest": "{{ params.driver_cpu }}",
        "coreLimit": "{{ params.driver_cpu }}",
        "memory": "{{ params.driver_memory }}",
        "serviceAccount": service_account,
    }
    if labels:
        spec["labels"] = labels
    if extra:
        spec.update(extra)
    return spec


def executor_spec(labels: dict | None = None, extra: dict | None = None) -> dict:
    """构造 SparkApplication 的 executor 段。CPU/内存走 params 渲染的字符串字段；
    实例数不写 CRD 的 instances（integer），改由 resource_spark_conf() 的
    spark.executor.instances 控制。"""
    spec = {
        "coreRequest": "{{ params.executor_cpu }}",
        "coreLimit": "{{ params.executor_cpu }}",
        "memory": "{{ params.executor_memory }}",
    }
    if labels:
        spec["labels"] = labels
    if extra:
        spec.update(extra)
    return spec
