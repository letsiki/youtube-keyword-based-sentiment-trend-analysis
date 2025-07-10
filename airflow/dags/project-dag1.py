from datetime import datetime
from airflow.decorators import task, task_group
from docker.types import Mount
from airflow import DAG
from datetime import datetime
from airflow.providers.discord.operators.discord_webhook import (
    DiscordWebhookOperator,
)
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException
from airflow.models.xcom_arg import XComArg

with DAG(
    dag_id="project-dag1",
    start_date=datetime(2025, 6, 28),
    schedule="8 9,15 * * *",
    catchup=False,
    tags=["bblue-project"],
) as dag:

    # --- DockerOperator Task

    scraper_task = DockerOperator(
        task_id="scraper_task",
        image="url-scraper:latest",
        auto_remove="force",
        tty=True,
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
        do_xcom_push=True,
        command="--debug",
        retrieve_output=True,
        retrieve_output_path="/airflow/xcom/return.pkl",
    )

    @task
    def create_chunks_out_of_urls(url_list, n_chunks):
        from math import ceil

        avg = ceil(len(url_list) / n_chunks)
        chunked_urls = [
            url_list[i * avg : (i + 1) * avg] for i in range(n_chunks)
        ]
        for chunk in chunked_urls:
            print(chunk)
        return chunked_urls

    @task_group
    def distributed_node(urls):
        @task
        def wait_a_bit_n_print(u):
            import time

            time.sleep(30)
            print(u)
            return u

        @task
        def print_twice(u):
            print(u + u)

        wait_a_bit_n_print_task = wait_a_bit_n_print(urls)
        wait_a_bit_n_print_task >> print_twice(wait_a_bit_n_print_task)

    # pick up xcom output of docker task
    scraper_output = XComArg(scraper_task)

    chunkify_task = create_chunks_out_of_urls(scraper_output, 4)

    mapped_groups = distributed_node.expand(urls=chunkify_task)

    scraper_task >> chunkify_task >> mapped_groups
