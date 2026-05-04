from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

with DAG(
    dag_id="etlt_pipeline",
    start_date=datetime(2026, 5, 1),
    schedule="0 0 * * *",
    catchup=False
) as dag:

    ingest = DockerOperator(
        task_id="ingest_data",
        image="apache/spark:4.0.2",
        container_name="spark_ingest_task",
        api_version="auto",
        auto_remove="success",
        user="root", 
        command=[
            "bash", "-c",
            (
                "mkdir -p /usr/app/.ivy/cache /usr/app/.ivy/jars && "
                "pip install --quiet psycopg2-binary && "
                "/opt/spark/bin/spark-submit "
                "--conf spark.jars.ivy=/usr/app/.ivy "
                "--packages org.apache.hadoop:hadoop-aws:3.4.0,"
                "com.amazonaws:aws-java-sdk-bundle:1.11.1026,"
                "org.postgresql:postgresql:42.7.3 "
                "/usr/app/ingest-data.py"
            )
        ],
        docker_url="unix://var/run/docker.sock",
        network_mode="my_network",
        mounts=[
            Mount(
            source="etlt-project_spark-apps",
            target="/usr/app",
            type="volume"
            )
        ],
        mount_tmp_dir=False,
        tty=True,
    )

    transform = DockerOperator(
    task_id="transform_data",
    image="ghcr.io/dbt-labs/dbt-postgres:1.9.0",
    container_name="dbt_transform_task",
    api_version="auto",
    auto_remove="force",
    command=["run", "--project-dir", "/usr/app/dbt", "--profiles-dir", "/usr/app/dbt"],
    docker_url="unix:///var/run/docker.sock",
    network_mode="my_network",
    mounts=[
        Mount(
            source="etlt-project_dbt-project",
            target="/usr/app/dbt",
            type="volume"
        )
    ],
    mount_tmp_dir=False,
    tty=True,
)

ingest >> transform