"""A minimal demo DAG.

Compatible with Airflow 2.x and 3.x. It runs two tasks in sequence using the
TaskFlow API: the first prints a greeting and returns a value, the second
consumes that value via XCom and prints the current time.
"""

from __future__ import annotations

import datetime

import pendulum
from airflow.decorators import dag, task


@dag(
    dag_id="hello_world",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["demo"],
)
def hello_world():
    @task
    def say_hello() -> str:
        message = "Hello from Airflow!"
        print(message)
        return message

    @task
    def print_time(message: str) -> None:
        print(f"{message} Current time is {datetime.datetime.now().isoformat()}")

    print_time(say_hello())


hello_world()
