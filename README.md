# airflow-dag-demo

Demo Airflow DAGs.

## Layout

- `dags/` — DAG definitions loaded by Airflow.
  - `hello_world.py` — a minimal two-task DAG (TaskFlow API), compatible with Airflow 2.x and 3.x.

## Usage

Point your Airflow deployment's DAGs folder at `dags/` (e.g. via git-sync when
using the official Helm chart), or copy the files into `$AIRFLOW_HOME/dags/`.
