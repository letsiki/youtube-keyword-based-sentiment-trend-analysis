from pathlib import Path
import os
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
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.models.xcom_arg import XComArg
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.models import TaskInstance


# -----------------------------DEFINE TASKS-----------------------------


@task()
def create_tables_if_not_exist():
    """runs airflow/include/create_tables.sql on the pg database"""
    sql_path = (
        Path(__file__).parent.parent / "include" / "create_tables.sql"
    )
    sql = sql_path.read_text()

    hook = PostgresHook(postgres_conn_id="my_postgres_conn")

    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
    existing_texts = hook.get_first(
        "SELECT array_agg(video_id) FROM text_from_audio;"
    )
    if None not in existing_texts:
        return ["v" + ex for ex in existing_texts[0]]
    else:
        return []


@task
def create_chunks_out_of_urls(n_chunks, **context):
    """
    split urls into chunks, to facilitate parallel processing
    """

    url_list = context["ti"].xcom_pull(task_ids="scraping.scraper_task")
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
def comments_to_db(**context):
    pass


@task
def fetch_comments(**context):
    pass


@task
def text_to_db(**context):
    """
    Connect to the db and write unique entries into the table
    Skips conflicts
    Deletes all txt files from the folder when done.
    """

    hook = PostgresHook("my_postgres_conn")

    text_files = Path("/opt/airflow/text")

    sql_path = (
        Path(__file__).parent.parent / "include" / "insert_text.sql"
    )
    data = text_to_list(text_files)

    sql = sql_path.read_text()

    try:
        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(sql, vars_list=data)
            conn.commit()
    except Exception as e:
        print(f"[ERROR] DB insert failed: {e}")
        raise AirflowFailException("Failed during batch insert")
    finally:
        # Always clean up
        for file in text_files.glob("*.txt"):
            try:
                os.remove(file)
            except Exception as e:
                print(f"[WARN] Could not delete {file}: {e}")


@task
def spark_operator_join_and_transform_to_sentiment(**context):
    pass


@task(trigger_rule=TriggerRule.ONE_FAILED, retries=0)
def watcher():
    """task that ensures that the dag run has failed whenever any of the tasks has failed"""
    raise AirflowException(
        "Failing task because one or more upstream tasks failed."
    )


# ------------------------DEFINE HELPER FUNCTIONS-----------------------


def file_count_check(ti: TaskInstance, folder_path: str) -> bool:
    """
    function used by wait_for_files PythonSensor to ensure that mp3 files
    are there prior to transcribing them.
    """
    # target_files_nr = len(
    #     ti.xcom_pull(task_ids="distributed_node.mp3_fetching")
    # )
    file_count = len(
        [
            f
            for f in os.listdir(folder_path)
            if os.path.isfile(os.path.join(folder_path, f))
        ]
    )
    return file_count >= 4


def text_to_list(path: Path | str) -> list[tuple]:
    """
    function that extracts all text files from a
    directory into a list of tuples
    format:
      [(filename1, col1), (filename2, col2), ..., (filename, coln)]
    """
    if isinstance(path, str):
        path = Path(path)
    data = []
    for txt in path.glob("*.txt"):
        vid_id = txt.stem
        vid_text = txt.read_text()
        data.append((vid_id, vid_text))
    return data


# -----------------------------DEFINE GROUPS----------------------------


@task_group
def mp3_fetching(urls):
    """
    mp3_fetching is responsible to for audio-text conversion and
    storing into a pg database
    """

    mp3_getter_task = DockerOperator(
        task_id="mp3_getter_task",
        image="mp3-getter:latest",
        auto_remove="never",
        tty=True,
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
        container_name="mp3_getter_{{ ti.map_index }}",
        command="--urls {{ ti.xcom_pull(task_ids='distributed_node.entry') | join(' ') }}",
        mounts=[
            Mount(
                source="/home/alex/Projects/bbd_project/sample-project-aa/mp3",
                target="/app/mp3",
                type="bind",
            ),
            Mount(
                source="/home/alex/Projects/bbd_project/sample-project-aa/json/video",
                target="/app/json/video",
                type="bind",
            ),
        ],
    )

    mp3_getter_task


@task_group
def comment_fetching(urls):
    """
    comment_fetching is responsible for scraping comments and
    storing them into a pg database.
    """

    fetch_comments() >> comments_to_db()


@task_group
def distributed_node(urls):
    """
    Container group for mp3_fetching and comment_fetching_group.
    Meant to be mapped into multiple instances to be executed in parallel.
    """

    entry(urls) >> [mp3_fetching(urls), comment_fetching(urls)]


@task_group
def scraping():
    """
    Group responsible for initiating a pg container, create tables if necessary
    and fetch all relevant urls (video id's) through scraping.
    """
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
        auto_remove="never",
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
def audio_to_text():
    """
    Group responsible for converting audio from youtube sources into text
    using openAI's whisper model.
    Stores results to pg database
    Depends indirectly (through a sensor) on task group mp3_fetching
    All mp3's must be gathered prior to processing.
    """

    wait_for_files = PythonSensor(
        task_id="wait_for_files",
        python_callable=file_count_check,
        op_kwargs={"folder_path": "/opt/airflow/mp3"},
        poke_interval=5,
        timeout=600,
        mode="poke",
    )

    audio_transcriber_task = DockerOperator(
        task_id="audio_transcriber",
        image="transcribe-many:latest",
        auto_remove="never",
        tty=True,
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
        command="--model tiny --urls {{ ti.xcom_pull(task_ids='scraping.scraper_task') | join(' ') }} --exclude  {{ ti.xcom_pull(task_ids='scraping.create_tables_if_not_exist') | join(' ') }}",
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

    wait_for_files >> audio_transcriber_task >> text_to_db()


# -----------------------------CREATE DAG-------------------------------

with DAG(
    dag_id="project-dag1",
    start_date=datetime(2025, 6, 28),
    schedule="5,35 * * * *",
    # schedule=None,
    catchup=False,
    tags=["bblue-project"],
) as dag:

    # removes all containers (volumes are kept)
    cleanup_pg = BashOperator(
        task_id="cleanup_pg",
        bash_command="docker rm -f airflow_pg_temp scraper_container whisper_container || true && docker ps -a --filter 'name=mp3_getter' | awk 'NR>1 {print $1}' | xargs -r docker rm -f || true && rm -f /opt/airflow/mp3/*",
    )

    chunkify_task = create_chunks_out_of_urls(8)

    spark_job = spark_operator_join_and_transform_to_sentiment()

    mapped_groups = distributed_node.expand(urls=chunkify_task)

    scraping_group = scraping()
    audio_to_text_group = audio_to_text()

    (
        scraping_group
        >> chunkify_task
        >> Label("Expand")
        >> mapped_groups
        >> spark_job
        >> cleanup_pg.as_teardown()
    )

    (
        scraping_group
        >> audio_to_text_group
        >> spark_job
        >> cleanup_pg.as_teardown()
    )

    spark_job >> watcher()
