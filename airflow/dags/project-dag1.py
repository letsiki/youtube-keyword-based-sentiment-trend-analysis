from datetime import datetime
from pathlib import Path
from math import ceil
from airflow.decorators import task, task_group
from airflow.sdk import Label
from docker.types import Mount
from airflow import DAG
from datetime import datetime
from airflow.providers.discord.operators.discord_webhook import (
    DiscordWebhookOperator,
)
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.state import State
from airflow.exceptions import AirflowException
from airflow.models.xcom_arg import XComArg
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.models import TaskInstance
import os

# -----------------------------DEFINE TASKS-----------------------------


@task
def create_tables_if_not_exist():
    sql_path = (
        Path(__file__).parent.parent / "include" / "create_tables.sql"
    )
    sql = sql_path.read_text()
    hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    hook.run(sql)


@task
def create_chunks_out_of_urls(n_chunks, **context):
    """
    split urls into chunks, to facilitate parallel processing
    prefix video_ids with 'v' for bash compatibility
    """

    url_list = context["ti"].xcom_pull(task_ids="tg1.scraper_task")
    avg = ceil(len(url_list) / n_chunks)
    chunked_urls = [
        [u for u in url_list][i * avg : (i + 1) * avg]
        for i in range(n_chunks)
    ]
    for chunk in chunked_urls:
        print(chunk)
    return chunked_urls


@task
def entry(u):
    """Entry point to distributed node"""
    return u


@task
def print_urls(**context):
    u = context["ti"].xcom_pull(task_ids="distributed_node.entry")
    print([url[1:] for url in u])


@task
def print_twice(**context):
    u = context["ti"].xcom_pull(task_ids="distributed_node.entry")
    print([2 * url[1:] for url in u])


@task(trigger_rule=TriggerRule.ONE_FAILED, retries=0)
def watcher():
    raise AirflowException(
        "Failing task because one or more upstream tasks failed."
    )


def file_count_check(ti: TaskInstance, folder_path: str) -> bool:
    min_files = len(ti.xcom_pull(task_ids="tg1.scraper_task"))
    file_count = len(
        [
            f
            for f in os.listdir(folder_path)
            if os.path.isfile(os.path.join(folder_path, f))
        ]
    )
    return file_count == min_files


# -----------------------------DEFINE GROUPS----------------------------


@task_group
def upper_branch(urls):
    """
    Upper branch is responsible to for audio-text conversion and
    storing into a pg database
    """

    mp3_getter_task = DockerOperator(
        task_id="mp3_getter_task",
        image="mp3-getter:latest",
        auto_remove="force",
        tty=True,
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
        container_name="mp3_getter_{{ ti.map_index }}",
        # do_xcom_push=True,
        command="--urls {{ ti.xcom_pull(task_ids='distributed_node.entry') | join(' ') }}",
        # retrieve_output=True,
        # retrieve_output_path="/airflow/xcom/return.pkl",
        mounts=[
            Mount(
                source="/home/alex/Projects/bbd_project/sample-project-aa/mp3",
                target="/app/mp3",
                type="bind",
            )
        ],
    )

    (
        # entry(urls)
        # >>
        mp3_getter_task
        # >> audio_transcriber_task
        >> print_urls()
    )


@task_group
def lower_branch(urls):
    """
    lower branch is responsible for scraping comments and
    storing them into a pg database.
    """

    # entry(urls) >>
    print_twice()


@task_group
def distributed_node(urls):
    """
    Container group of upper_branch and lower_branch groups.
    """

    entry(urls) >> [upper_branch(urls), lower_branch(urls)]
    # upper_branch(urls)
    # lower_branch(urls)


@task_group
def tg1():
    # creates a pg instance mounting it to postgres_volume_project
    start_pg = BashOperator(
        task_id="start_pg",
        bash_command=(
            "docker run -d --name airflow_pg_temp "
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
        container_name="scraper_container",
    )

    (
        start_pg.as_setup()
        >> create_tables_if_not_exist()
        >> scraper_task
    )


@task_group
def tg2():
    wait_for_files = PythonSensor(
        task_id="wait_for_files",
        python_callable=file_count_check,
        op_kwargs={"folder_path": "/mp3"},
        poke_interval=5,
        timeout=600,
        mode="poke",
    )

    audio_transcriber_task = DockerOperator(
        task_id="audio_transcriber",
        image="transcribe-many:latest",
        auto_remove="force",
        tty=True,
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
        # do_xcom_push=True,
        command="--model tiny --urls {{ ti.xcom_pull(task_ids='tg1.scraper_task') | join(' ') }}",
        # retrieve_output=True,
        # retrieve_output_path="/airflow/xcom/return.pkl",
        container_name="whisper_container",
        mounts=[
            Mount(
                source="/home/alex/Projects/bbd_project/sample-project-aa/mp3",
                target="/app/mp3",
                type="bind",
            ),
            Mount(
                source="/home/alex/Projects/bbd_project/sample-project-aa/text",
                target="/app/text",
                type="bind",
            ),
        ],
    )

    wait_for_files >> audio_transcriber_task


# -----------------------------CREATE DAG-------------------------------

with DAG(
    dag_id="project-dag1",
    start_date=datetime(2025, 6, 28),
    # schedule="8 9,15 * * *",
    schedule=None,
    catchup=False,
    tags=["bblue-project"],
) as dag:

    # removes the pg docker container (volume is kept)
    cleanup_pg = BashOperator(
        task_id="cleanup_pg",
        bash_command="docker rm -f airflow_pg_temp scraper_container whisper_container && docker ps -a --filter 'name=mp3_getter' | awk 'NR>1 {print $1}' | xargs -r docker rm -f || true && rm -f /mp3/*",
    )

    chunkify_task = create_chunks_out_of_urls(4)

    mapped_groups = distributed_node.expand(urls=chunkify_task)

    tg1_group = tg1()
    tg2_group = tg2()

    (
        tg1_group
        >> chunkify_task
        >> Label("Expand")
        >> mapped_groups
        >> cleanup_pg.as_teardown()
    )

    tg1_group >> tg2_group >> cleanup_pg.as_teardown()

    [tg2_group, mapped_groups] >> watcher()
