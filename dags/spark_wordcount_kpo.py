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

WordCount 用自写的 Java 应用（com.example.spark.SparkWordCount）执行，input/output 作为
spark-submit 的 application args（命令行位置参数）传入：`spark-submit --class ... app.jar <input> <output>`。
gluten 镜像仅含 JRE（无 python、无 javac），故用「预编译好的 jar」而非 PySpark 脚本或容器内编译；
jar 从公开 raw URL 拉取（cluster mode 下由 spark-submit 自动分发给独立的 driver pod）。

KPO 建的任务 Pod 独立、无 gitSync，因此应用 jar（wordcount-app.jar）从公开 raw URL 拉取。
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

from kubernetes.client import models as k8s

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
# 自写的 Java WordCount 应用 jar（com.example.spark.SparkWordCount，吃 <input> <output>）公开 raw 地址。
# gluten 镜像仅含 JRE（无 python/javac），故用预编译 jar；KPO Pod 无 gitSync，运行时下载。
SPARK_APP_JAR_URL = os.getenv(
    "SPARK_APP_JAR_URL",
    "https://raw.githubusercontent.com/wgqcd88/airflow-dag-demo/main/dags/spark_apps/wordcount-app.jar",
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

# 容器内脚本：定位 spark-submit、下载自写的 WordCount jar、提交（input/output 作为命令行参数）。
CONTAINER_SCRIPT = r"""
set -euo pipefail
# gluten 镜像仅含 JRE（无 python、无 javac），故用「预编译好的 Java WordCount jar」，
# 运行时从仓库 raw URL 拉取；input/output 作为 spark-submit 的 application args（命令行位置参数）传入。
SPARK_SUBMIT="$(command -v spark-submit || true)"
if [ -z "$SPARK_SUBMIT" ]; then SPARK_SUBMIT="${SPARK_HOME:-/opt/spark}/bin/spark-submit"; fi
fetch() {
  if command -v curl >/dev/null 2>&1; then curl -fsSL -o "$2" "$1"
  elif command -v wget >/dev/null 2>&1; then wget -q -O "$2" "$1"
  else echo "no curl/wget to fetch $1" >&2; return 1
  fi
}
# 拉取自写的 WordCount 应用 jar（含类 com.example.spark.SparkWordCount，吃 <input> <output>）。
APP_JAR=/tmp/wordcount-app.jar
echo "下载应用 jar: $SPARK_APP_JAR_URL -> $APP_JAR"
fetch "$SPARK_APP_JAR_URL" "$APP_JAR"
# input/output 直接透传给应用作为命令行参数：
#   - 本地/URL 输入：cluster mode 下 driver 在独立 pod，无法读提交端本地文件，故 http(s) 输入
#     交给 Spark 自身用 --files 分发或由应用直接读 URL；这里保持透传，spark.read.text 支持 http/abfss/file。
#   - abfss/hdfs 路径：透传，由 ABFS OAuth（Workload Identity）鉴权。
IN="$SPARK_INPUT"
OUT="$SPARK_OUTPUT"
# Spark on Kubernetes cluster mode: KPO 只 launch driver pod；driver 在独立 K8s pod 跑并起 executor。
CONF_ARGS=()
DEPLOY_MODE_ARGS=()
case "$SPARK_MASTER" in
  k8s://*)
    NS_SELF="$(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace 2>/dev/null || echo data-platform)"
    DEPLOY_MODE_ARGS+=(--deploy-mode cluster)
    CONF_ARGS+=(--conf "spark.kubernetes.namespace=$NS_SELF")
    CONF_ARGS+=(--conf "spark.kubernetes.authenticate.driver.serviceAccountName=${SPARK_KPO_SA:-spark-sa}")
    # 让 KPO 阻塞直到 driver 完成 —— 否则 KPO 会瞬间退出而 driver 还在跑。
    CONF_ARGS+=(--conf "spark.kubernetes.submission.waitAppCompletion=true")
    # driver + executor 用同镜像（含 gluten）。
    if [ -n "${SPARK_KPO_IMAGE:-}" ]; then
      CONF_ARGS+=(--conf "spark.kubernetes.container.image=$SPARK_KPO_IMAGE")
    fi
    # driver 也带 WI label；executor 同样。
    CONF_ARGS+=(--conf "spark.kubernetes.driver.label.azure.workload.identity/use=true")
    CONF_ARGS+=(--conf "spark.kubernetes.executor.label.azure.workload.identity/use=true")
    # cluster mode driver 完成后保留 pod 便于查日志（改 delete 需 KPO app pod finish_action + spark 侧联动）。
    CONF_ARGS+=(--conf "spark.kubernetes.driver.pod.deletionPolicy=OnCompletion")
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
echo "应用 jar: $APP_JAR (class=com.example.spark.SparkWordCount)"
# cluster mode 下 driver 在独立 pod，提交端本地的 $APP_JAR 它读不到，故主 app jar 用远程 URL，
# spark-submit 会自动下载分发给 driver；client/local 模式则用已下载的本地路径。
case "${DEPLOY_MODE_ARGS[*]-}" in
  *"--deploy-mode cluster"*) APP_JAR_REF="$SPARK_APP_JAR_URL" ;;
  *) APP_JAR_REF="$APP_JAR" ;;
esac
echo "提交命令: $SPARK_SUBMIT --master $SPARK_MASTER ${DEPLOY_MODE_ARGS[*]-} [ABFS-WI=$ABFS_WI] --class com.example.spark.SparkWordCount $APP_JAR_REF (input=$IN output=$OUT)"
# input/output 作为 application args（命令行位置参数）传给 SparkWordCount：args[0]=input, args[1]=output。
exec "$SPARK_SUBMIT" --master "$SPARK_MASTER" \
  ${DEPLOY_MODE_ARGS[@]+"${DEPLOY_MODE_ARGS[@]}"} \
  ${CONF_ARGS[@]+"${CONF_ARGS[@]}"} \
  --conf spark.sql.warehouse.dir=/tmp/spark-warehouse \
  --class com.example.spark.SparkWordCount \
  "$APP_JAR_REF" "$IN" "$OUT"
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
        "app_jar_url": Param(
            SPARK_APP_JAR_URL,
            type="string",
            title="应用 jar URL",
            description=(
                "WordCount 应用 jar 的下载地址。默认 main 分支 raw URL——注意 GitHub raw "
                "对 mutable 分支路径有约 5 分钟 CDN 缓存，刚 push 新 jar 后立即触发可能拉到旧版。"
                "如需立即用某次提交的 jar，改用不可变的 commit SHA 路径："
                "https://raw.githubusercontent.com/wgqcd88/airflow-dag-demo/<commit-sha>/dags/spark_apps/wordcount-app.jar"
            ),
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
        env_vars=[
            k8s.V1EnvVar(name="SPARK_MASTER", value="{{ params.master }}"),
            k8s.V1EnvVar(name="SPARK_INPUT", value="{{ params.input }}"),
            k8s.V1EnvVar(name="SPARK_OUTPUT", value="{{ params.output }}"),
            k8s.V1EnvVar(name="SPARK_APP_URL", value=SPARK_APP_URL),
            k8s.V1EnvVar(name="SPARK_APP_JAR_URL", value="{{ params.app_jar_url }}"),
            k8s.V1EnvVar(name="SPARK_ADLS_HOST", value=ADLS_HOST),
            k8s.V1EnvVar(name="SPARK_KPO_SA", value="{{ params.sa }}"),
            k8s.V1EnvVar(name="SPARK_KPO_IMAGE", value="{{ params.image }}"),
            k8s.V1EnvVar(
                name="POD_IP",
                value_from=k8s.V1EnvVarSource(
                    field_ref=k8s.V1ObjectFieldSelector(field_path="status.podIP"),
                ),
            ),
        ],
        get_logs=True,
        in_cluster=True,
        on_finish_action="delete_pod",
        image_pull_policy="IfNotPresent",
        startup_timeout_seconds=600,
    )
