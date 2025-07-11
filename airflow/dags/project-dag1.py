from datetime import datetime
from math import ceil
from airflow.decorators import task, task_group
from docker.types import Mount
from airflow import DAG
from datetime import datetime
from airflow.providers.discord.operators.discord_webhook import (
    DiscordWebhookOperator,
)
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException
from airflow.models.xcom_arg import XComArg

with DAG(
    dag_id="project-dag1",
    start_date=datetime(2025, 6, 28),
    # schedule="8 9,15 * * *",
    schedule=None,
    catchup=False,
    tags=["bblue-project"],
) as dag:

    # ---TASK DEFINITION---

    @task
    def create_chunks_out_of_urls(n_chunks, **context):
        """
        split urls into chunks, to facilitate parallel processing
        prefix video_ids with 'v' for bash compatibility
        """

        url_list = context["ti"].xcom_pull(task_ids="scraper_task")
        avg = ceil(len(url_list) / n_chunks)
        chunked_urls = [
            ["v" + u for u in url_list][i * avg : (i + 1) * avg]
            for i in range(n_chunks)
        ]
        for chunk in chunked_urls:
            print(chunk)
        return chunked_urls

    @task_group
    def upper_branch(urls):
        """
        Upper branch is responsible to for audio-text conversion and
        storing into a pg database
        """

        # ---TASK DEFINITION---

        @task
        def entry(u):
            return u

        @task
        def print_urls(**context):
            print(
                context["ti"].xcom_pull(
                    task_ids="distributed_node.upper_branch.mp3_getter_task"
                )
            )

        # ---TASK CREATION---

        mp3_getter_task = DockerOperator(
            task_id="mp3_getter_task",
            image="mp3-getter:latest",
            auto_remove="force",
            tty=True,
            mount_tmp_dir=False,
            docker_url="unix://var/run/docker.sock",
            do_xcom_push=True,
            command="--urls {{ ti.xcom_pull(task_ids='distributed_node.upper_branch.entry')}}",
            retrieve_output=True,
            retrieve_output_path="/airflow/xcom/return.pkl",
            mounts=[
                Mount(
                    source="/home/alex/Projects/bbd_project/sample-project-aa/mp3",
                    target="/app/downloads/160",
                    type="bind",
                )
            ],
        )

        (entry(urls) >> mp3_getter_task >> print_urls())

    @task_group
    def lower_branch(urls):
        """
        lower branch is responsible for scraping comments and
        storing them into a pg database.
        """

        @task
        def print_twice(u):
            print([2 * url for url in u])

        print_twice(urls)

    @task_group
    def distributed_node(urls):
        """
        Container group of upper_branch and lower_branch groups.
        """

        # ---TASK CREATION---

        upper_branch(urls)
        lower_branch(urls)

    # ---TASK CREATION---

    # removes leftovers and creates a pg instance mounting it to postgres_volume_project
    start_pg = BashOperator(
        task_id="start_pg",
        bash_command=(
            "(docker rm -f airflow_pg_temp || true) && docker run -d --name airflow_pg_temp "
            "--network airflow_default "  # or whatever network Airflow uses
            "-e POSTGRES_USER=airflow "
            "-e POSTGRES_PASSWORD=airflow "
            "-e POSTGRES_DB=project_data "
            "-v postgres_volume_project:/var/lib/postgresql/data "
            "postgres:15"
        ),
    )

    # user keywords to find playlists and return all urls
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

    # removes the pg docker container (volume is kept)
    cleanup_pg = BashOperator(
        task_id="cleanup_pg",
        bash_command="docker rm -f airflow_pg_temp || true",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    chunkify_task = create_chunks_out_of_urls(8)

    mapped_groups = distributed_node.expand(urls=chunkify_task)

    (
        start_pg
        >> scraper_task
        >> chunkify_task
        >> mapped_groups
        >> cleanup_pg
    )
