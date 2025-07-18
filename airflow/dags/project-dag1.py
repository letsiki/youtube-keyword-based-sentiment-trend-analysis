from pathlib import Path
import os
import json
from datetime import datetime
from pathlib import Path
from math import ceil
from airflow.decorators import task, task_group
from airflow.sdk import Label
from docker.types import Mount
from airflow import DAG
from datetime import datetime
import time
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
from airflow.models.param import Param


# -----------------------------DEFINE TASKS-----------------------------


@task
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
def create_chunks_out_of_urls(**context):
    """
    split urls into chunks, to facilitate parallel processing
    """
    n_chunks = context["params"]["worker_nr"]
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
    """
    Connect to the db and write comments from the json files
    generated earler.
    (video id, timestamp) compososite key, serves as the primary key
    """

    hook = PostgresHook("my_postgres_conn")

    comment_dir = Path("/opt/airflow/json/comments")

    sql_path = (
        Path(__file__).parent.parent / "include" / "insert_comments.sql"
    )

    sql = sql_path.read_text()

    json_dir = Path("/opt/airflow/json/comments")

    data = comm_json_to_list(json_dir)

    try:
        if not data:
            print("[WARN] No data to insert.")
            return  # Avoid trying to insert if no data

        # Check if data is valid
        for entry in data:
            if len(entry) != 2:
                print(
                    f"[ERROR] Invalid data format: {entry[:30] if isinstance(entry, str) else entry}"
                )
                raise ValueError(f"Invalid data format: {entry}")
        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                print("[INFO] Executing batch insert...")
                cursor.executemany(sql, vars_list=data)
                conn.commit()
            print(f"Inserted {len(data)} into the database")
            print("[INFO] Data inserted successfully.")
    except Exception as e:
        print(f"[ERROR] DB insert failed: {e}")
        raise AirflowFailException("Failed during batch insert")


@task
def text_and_metadata_to_db(**context):
    """
    Connect to the db and write unique entries into the table
    Skips conflicts
    Deletes all txt files from the folder when done.
    """

    hook = PostgresHook("my_postgres_conn")

    text_dir = Path("/opt/airflow/text")
    json_dir = Path("/opt/airflow/json/video")

    sql_path = (
        Path(__file__).parent.parent / "include" / "insert_text.sql"
    )
    data = text_to_list(text_dir)

    metadata = meta_json_to_list(
        [f"{json_dir}/{entry[0]}.json" for entry in data]
    )

    # Convert the lists into dictionaries with the first element as the key
    dict1 = {t[0]: t[1:] for t in metadata}
    dict2 = {t[0]: t[1:] for t in data}

    full_data = [
        (key, *dict1[key], *dict2[key]) for key in dict1 if key in dict2
    ]
    print(f"found {len(metadata)} entries in metadata")
    print(f"found {len(data)} entries in data")
    print("Full data prepared for insertion:")
    print(
        list(
            map(
                lambda x: x[:30] if isinstance(x, str) else x, full_data
            )
        )
    )

    sql = sql_path.read_text()

    try:
        if not full_data:
            print("[WARN] No data to insert.")
            return  # Avoid trying to insert if no data

        # Check if data is valid
        for entry in full_data:
            if len(entry) != 7:
                print(
                    f"[ERROR] Invalid data format: {entry[:30] if isinstance(entry, str) else entry}"
                )
                raise ValueError(f"Invalid data format: {entry}")

        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                print("[INFO] Executing batch insert...")
                cursor.executemany(sql, vars_list=full_data)
            conn.commit()
            print(f"Inserted {len(full_data)} into the database")
            print("[INFO] Data inserted successfully.")
    except Exception as e:
        print(f"[ERROR] DB insert failed: {e}")
        raise AirflowFailException("Failed during batch insert")
    finally:
        # Always clean up - optional happens at the dag clean up too
        for file in text_dir.glob("*.txt"):
            try:
                os.remove(file)
            except Exception as e:
                print(f"[WARN] Could not delete {file}: {e}")
        for file in json_dir.glob("*.json"):
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
    Returns True if there is at least one .mp3 file
    and the count has stayed the same for at least 60 seconds.
    """
    current_count = len(list(Path(folder_path).glob("*.mp3")))
    now = time.time()

    if current_count < 1:
        return False

    # Retrieve previous count and timestamp
    prev_data = (
        ti.xcom_pull(task_ids=ti.task_id, key="file_count_state") or {}
    )
    prev_count = prev_data.get("count")
    prev_time = prev_data.get("timestamp")

    # If count changed or this is first run → update and wait
    if prev_count != current_count or prev_time is None:
        ti.xcom_push(
            key="file_count_state",
            value={"count": current_count, "timestamp": now},
        )
        return False

    # If count stayed the same for over a minute → we're good
    if (now - prev_time) >= 60:
        return True

    return False


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


def comm_json_to_list(path: Path | str) -> list[tuple]:
    """
    function that extracts all text files from a
    directory into a list of tuples
    format:
      [(filename1, col1), (filename2, col2), ..., (filename, coln)]
    """
    if isinstance(path, str):
        path = Path(path)
    data = []
    for json_ in path.glob("*.json"):
        vid_id = json_.stem
        with open(json_, "r") as f:
            vid_dict = json.load(f)
        data.append((vid_id, json.dumps(vid_dict)))
    return data


def meta_json_to_list(json_filepaths: list[str]) -> list[tuple]:
    """
    function that extracts all json files from a
    directory into a list of tuples
    format:
        [...]
    """
    data = []
    for json_ in json_filepaths:
        # print(json_filepaths)
        with open(json_, "r") as f:
            json_dict = json.load(f)
        # Convert string to date object
        date_obj = datetime.strptime(
            json_dict["upload_date"], "%Y%m%d"
        ).date()
        data.append(
            (
                os.path.basename(json_).split(".")[0],
                json_dict["title"],
                json_dict["uploader"],
                json_dict["description"],
                json_dict["view_count"],
                date_obj,
            )
        )
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
        environment={"LOGICAL_DATE": "{{ ds | replace('-', '') }}"},
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

    fetch_comments = DockerOperator(
        task_id="fetch_comments",
        image="comment-scraper-2:latest",
        auto_remove="never",
        tty=True,
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
        container_name="comment-scraper2_{{ ti.map_index }}",
        command="--urls {{ ti.xcom_pull(task_ids='distributed_node.entry') | join(' ') }}",
        # environment={"LOGICAL_DATE": "{{ ds | replace('-', '') }}"},
        mounts=[
            Mount(
                source="/home/alex/Projects/bbd_project/sample-project-aa/json/comments",
                target="/app/json/comments",
                type="bind",
            ),
        ],
    )

    fetch_comments >> comments_to_db()


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
        soft_fail=True,
        timeout=900,
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

    (
        wait_for_files
        >> audio_transcriber_task
        >> text_and_metadata_to_db()
    )


# -----------------------------CREATE DAG-------------------------------

with DAG(
    dag_id="project-dag1",
    start_date=datetime(2025, 6, 28),
    # schedule="0-59/7 * * * *",
    schedule=None,
    # schedule="59 * * * * *",
    max_active_runs=1,
    params={"worker_nr": Param(9, type="integer")},
    catchup=False,
    tags=["bblue-project"],
) as dag:

    # notify on error
    discord_notify_task = DiscordWebhookOperator(
        task_id="notify_discord",
        trigger_rule=TriggerRule.ONE_FAILED,
        message="""
    🚨 *DAG Failed!*
    - DAG: `{{ dag.dag_id }}`
    - Run ID: `{{ run_id }}`
    - Execution date: `{{ ds }}`
    """,
        http_conn_id="discord_conn_id",
    )

    # removes all containers (volumes are kept)
    cleanup_pg = BashOperator(
        trigger_rule=TriggerRule.ALL_DONE,
        task_id="cleanup_pg",
        bash_command="docker rm -f airflow_pg_temp scraper_container whisper_container || true && docker ps -a --filter 'name=mp3_getter' | awk 'NR>1 {print $1}' | xargs -r docker rm -f || true && docker ps -a --filter 'name=comment-scraper2' | awk 'NR>1 {print $1}' | xargs -r docker rm -f || true && rm -f /opt/airflow/mp3/* && rm -f /opt/airflow/json/video/* && rm -f /opt/airflow/json/comments/* && rm -f /opt/airflow/text/*",
    )

    chunkify_task = create_chunks_out_of_urls()

    spark_job = spark_operator_join_and_transform_to_sentiment()

    mapped_groups = distributed_node.expand(urls=chunkify_task)

    scraping_group = scraping()
    audio_to_text_group = audio_to_text()

    cleanup_pg.as_teardown()

    (
        scraping_group
        >> chunkify_task
        >> Label("Expand")
        >> mapped_groups
        >> spark_job
    )

    (scraping_group >> audio_to_text_group >> spark_job)

    [spark_job, audio_to_text_group] >> cleanup_pg

    spark_job >> watcher() >> discord_notify_task
