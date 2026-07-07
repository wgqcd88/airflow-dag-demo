"""Spark WordCount 示例 DAG（KubernetesPodOperator，镜像/参数均可按触发覆盖）。

相比 BashOperator + executor_config.pod_override：KubernetesExecutor 的 executor_config
不会做 Jinja 模板渲染（pod_override 里的 image 会变成字面量 "{{ params.image }}"，导致
ImagePullBackOff），因此无法按「本次触发」切换镜像。KubernetesPodOperator 的 image / cmds /
arguments / env_vars 都是模板字段，可以真正用 DAG 参数 image 在每次触发时指定 worker 镜像。

参数（params）触发时可在 UI「Trigger DAG w/ config」或 REST/CLI 的 --conf 覆盖：
    - image  : 任务 Pod 镜像（需含 Spark/Java），从 Spark 3.4.4 / 3.5.4 / 4.0.1 Gluten 镜像中选择
    - master : Spark master，默认集群内 Kubernetes API 地址 k8s://https://kubernetes.default.svc:443
    - input  : 输入路径，默认 GitHub raw URL（也支持 ADLS Gen2 / 本地路径 / http(s) URL）
    - output : 输出目录，默认 /tmp/output（也支持 ADLS Gen2 abfss:// 对象存储）
    - cpu    : KPO 任务 Pod CPU request/limit，默认 1
    - memory : KPO 任务 Pod 内存 request/limit，默认 2Gi
    - sa     : KPO 任务 Pod 使用的 Kubernetes ServiceAccount，默认 spark-sa

读写对象存储：input/output 若用 ADLS Gen2，则路径格式为 abfss://<容器>@<账户>.dfs.core.windows.net/...。
鉴权用 Azure Workload Identity——KPO Pod 以 spark-sa SA 运行并带
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

# 可选 worker 镜像（含 Spark + Gluten/Velox）。触发 DAG 时通过 image 参数选择。
WORKER_IMAGE_CHOICES = (
    "ghcr.io/wgqcd88/spark:3.4.4-gluten-20260705",
    "ghcr.io/wgqcd88/spark:3.5.4-gluten-20260705",
    "ghcr.io/wgqcd88/spark:4.0.1-gluten-20260705",
)
WORKER_IMAGE_ENV = os.getenv("SPARK_WORKER_IMAGE", WORKER_IMAGE_CHOICES[-1])
WORKER_IMAGE = (
    WORKER_IMAGE_ENV
    if WORKER_IMAGE_ENV in WORKER_IMAGE_CHOICES
    else WORKER_IMAGE_CHOICES[-1]
)
# KPO 任务 Pod 所在命名空间（worker SA 已有在此建 Pod 的 RBAC）。
KPO_NAMESPACE = os.getenv("SPARK_KPO_NAMESPACE", "data-platform")
# 默认 Spark master 使用集群内 Kubernetes API 地址；触发参数仍可覆盖。
DEFAULT_MASTER = os.getenv(
    "SPARK_DEFAULT_MASTER",
    "k8s://https://kubernetes.default.svc:443",
)
# Spark 应用（wordcount.py）公开 raw 地址——KPO Pod 无 gitSync，运行时下载。
SPARK_APP_URL = os.getenv(
    "SPARK_APP_URL",
    "https://raw.githubusercontent.com/wgqcd88/airflow-dag-demo/main/dags/spark_apps/wordcount.py",
)
# ADLS Gen2 账户 host（用于 ABFS OAuth 配置键）。为空则不注入 ABFS 配置（如纯本地/URL 输入）。
ADLS_HOST = os.getenv("SPARK_ADLS_HOST", "wgqjesa.dfs.core.windows.net")
# 默认输入使用 GitHub raw URL；如 input/output 改用 ADLS Gen2 / abfss，则用 Workload Identity 鉴权。
DEFAULT_INPUT = os.getenv(
    "SPARK_DEFAULT_INPUT",
    "https://raw.githubusercontent.com/wgqcd88/airflow-dag-demo/main/dags/spark_apps/wordcount.py",
)
DEFAULT_OUTPUT = os.getenv(
    "SPARK_DEFAULT_OUTPUT",
    "/tmp/output",
)
# KPO 任务 Pod 使用的 SA（带 azure.workload.identity/client-id 注解）。
DEFAULT_CPU = os.getenv("SPARK_KPO_CPU", "1")
DEFAULT_MEMORY = os.getenv("SPARK_KPO_MEMORY", "2Gi")
DEFAULT_SA = os.getenv("SPARK_WORKER_SA", "spark-sa")

# 容器内脚本：定位 spark-submit、下载 app（必要时下载 URL 输入）、提交。
CONTAINER_SCRIPT = r"""
set -euo pipefail
# gluten 镜像仅含 JRE（无 python），故走 spark-examples 里的 JavaWordCount（Java 类）。
SPARK_SUBMIT="$(command -v spark-submit || true)"
if [ -z "$SPARK_SUBMIT" ]; then SPARK_SUBMIT="${SPARK_HOME:-/opt/spark}/bin/spark-submit"; fi
# 定位 examples jar（版本随镜像变动，用通配 + head）。
EX_JAR="$(ls -1 ${SPARK_HOME:-/opt/spark}/examples/jars/spark-examples_*.jar 2>/dev/null | head -1 || true)"
if [ -z "$EX_JAR" ]; then
  echo "spark-examples jar not found under ${SPARK_HOME:-/opt/spark}/examples/jars/" >&2
  exit 3
fi
fetch() {
  if command -v curl >/dev/null 2>&1; then curl -fsSL -o "$2" "$1"
  elif command -v wget >/dev/null 2>&1; then wget -q -O "$2" "$1"
  else echo "no curl/wget to fetch $1" >&2; return 1
  fi
}
# HTTP(S) 输入落到本地文件；abfss/hdfs/local 路径直接透传给 spark。
IN="$SPARK_INPUT"
case "$IN" in
  http://*|https://*) fetch "$IN" /tmp/spark_input_data; IN=/tmp/spark_input_data ;;
esac
# Spark on Kubernetes: driver 起 executor 时须知 namespace/SA/镜像；否则默认 default+spark 会 403。
CONF_ARGS=()
case "$SPARK_MASTER" in
  k8s://*)
    NS_SELF="$(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace 2>/dev/null || echo data-platform)"
    CONF_ARGS+=(--conf "spark.kubernetes.namespace=$NS_SELF")
    CONF_ARGS+=(--conf "spark.kubernetes.authenticate.driver.serviceAccountName=${SPARK_KPO_SA:-spark-sa}")
    # executor 用同镜像（含 gluten）；driver 自己也用该镜像。
    if [ -n "${SPARK_KPO_IMAGE:-}" ]; then
      CONF_ARGS+=(--conf "spark.kubernetes.container.image=$SPARK_KPO_IMAGE")
    fi
    # executor pod 也带 WI label，方便读 abfss。
    CONF_ARGS+=(--conf "spark.kubernetes.executor.label.azure.workload.identity/use=true")
    ;;
esac
# 若目标是 ADLS Gen2（abfss）且注入了 Workload Identity 环境变量，则拼 ABFS OAuth 配置。
ABFS_WI="off"
if [ -n "${SPARK_ADLS_HOST:-}" ] && [ -n "${AZURE_CLIENT_ID:-}" ] && [ -n "${AZURE_FEDERATED_TOKEN_FILE:-}" ]; then
  H="$SPARK_ADLS_HOST"
  CONF_ARGS+=(--conf "spark.hadoop.fs.azure.account.auth.type.$H=OAuth")
  CONF_ARGS+=(--conf "spark.hadoop.fs.azure.account.oauth.provider.type.$H=org.apache.hadoop.fs.azurebfs.oauth2.WorkloadIdentityTokenProvider")
  CONF_ARGS+=(--conf "spark.hadoop.fs.azure.account.oauth2.msi.tenant.$H=$AZURE_TENANT_ID")
  CONF_ARGS+=(--conf "spark.hadoop.fs.azure.account.oauth2.client.id.$H=$AZURE_CLIENT_ID")
  CONF_ARGS+=(--conf "spark.hadoop.fs.azure.account.oauth2.token.file.$H=$AZURE_FEDERATED_TOKEN_FILE")
  ABFS_WI="on(account=$H client=$AZURE_CLIENT_ID)"
fi
echo "ABFS Workload Identity: $ABFS_WI"
echo "spark-examples jar: $EX_JAR"
echo "提交命令: $SPARK_SUBMIT --master $SPARK_MASTER --class org.apache.spark.examples.JavaWordCount [ABFS-WI=$ABFS_WI] $EX_JAR $IN"
# 注意：JavaWordCount 只 println 到 driver stdout，SPARK_OUTPUT 参数不使用。
exec "$SPARK_SUBMIT" --master "$SPARK_MASTER" \
  --class org.apache.spark.examples.JavaWordCount \
  ${CONF_ARGS[@]+"${CONF_ARGS[@]}"} \
  "$EX_JAR" "$IN"
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
            description="任务 Pod 镜像（需含 Spark/Java），从固定 Gluten 镜像列表中选择",
            enum=list(WORKER_IMAGE_CHOICES),
        ),
        "master": Param(
            DEFAULT_MASTER,
            type="string",
            title="Spark master",
            description="默认集群内 Kubernetes API 地址；也可改为 local[*] / spark://host:7077 / yarn",
        ),
        "input": Param(
            DEFAULT_INPUT,
            type="string",
            title="输入路径",
            description="默认 GitHub raw URL；也支持 ADLS Gen2 abfss:// / 本地路径 / http(s) URL",
        ),
        "output": Param(
            DEFAULT_OUTPUT,
            type="string",
            title="输出目录",
            description="默认 /tmp/output；也支持 ADLS Gen2 abfss:// 对象存储",
        ),
        "cpu": Param(
            DEFAULT_CPU,
            type="string",
            title="CPU",
            description="KPO 任务 Pod CPU request/limit，如 500m / 1 / 2",
        ),
        "memory": Param(
            DEFAULT_MEMORY,
            type="string",
            title="内存",
            description="KPO 任务 Pod 内存 request/limit，如 1Gi / 2Gi / 4Gi",
        ),
        "sa": Param(
            DEFAULT_SA,
            type="string",
            title="ServiceAccount",
            description="KPO 任务 Pod 使用的 Kubernetes ServiceAccount",
        ),
    },
) as dag:
    submit = KubernetesPodOperator(
        task_id="submit",
        name="spark-wordcount-kpo",
        namespace=KPO_NAMESPACE,
        labels={"azure.workload.identity/use": "true"},
        pod_template_dict={
            "spec": {
                "serviceAccountName": "{{ params.sa }}",
                "containers": [
                    {
                        "name": "base",
                        "resources": {
                            "requests": {
                                "cpu": "{{ params.cpu }}",
                                "memory": "{{ params.memory }}",
                            },
                            "limits": {
                                "cpu": "{{ params.cpu }}",
                                "memory": "{{ params.memory }}",
                            },
                        },
                    },
                ],
            },
        },
        image="{{ params.image }}",
        cmds=["bash", "-c", CONTAINER_SCRIPT],
        env_vars={
            "SPARK_MASTER": "{{ params.master }}",
            "SPARK_INPUT": "{{ params.input }}",
            "SPARK_OUTPUT": "{{ params.output }}",
            "SPARK_APP_URL": SPARK_APP_URL,
            "SPARK_ADLS_HOST": ADLS_HOST,
            "SPARK_KPO_SA": "{{ params.sa }}",
            "SPARK_KPO_IMAGE": "{{ params.image }}",
        },
        get_logs=True,
        in_cluster=True,
        on_finish_action="delete_pod",
        image_pull_policy="IfNotPresent",
        startup_timeout_seconds=600,
    )
