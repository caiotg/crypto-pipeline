import json
from datetime import datetime, timedelta, timezone
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from docker.types import Mount

from plugins.coingecko_client import CoinGeckoClient

default_args = {
    "owner": "caio",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="crypto_market_elt",
    schedule="0 */6 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=default_args,
    tags=["crypto", "elt", "portfolio"],
)
def crypto_market_elt():

    @task
    def extract_markets() -> list[dict]:
        client = CoinGeckoClient()
        return client.get_markets(per_page=100, page=1)

    @task
    def load_raw(records: list[dict]):
        df = pd.DataFrame(records)
        df["roi"] = df["roi"].apply(lambda x: json.dumps(x) if isinstance(x, dict) else None)
        df["ingested_at"] = datetime.now(timezone.utc)

        hook = PostgresHook(postgres_conn_id="analytics_postgres")
        engine = hook.get_sqlalchemy_engine()
        df.to_sql("raw_coins_market", engine, schema="raw", if_exists="append", index=False)

    run_dbt = DockerOperator(
        task_id="run_dbt",
        image="crypto-dbt:latest",
        command="run --profiles-dir /root/.dbt --project-dir /usr/app/dbt",
        docker_url="unix://var/run/docker.sock",
        network_mode="crypto-pipeline_default",
        mounts=[
            Mount(source="/c/Users/Admin/Documents/dev/crypto-pipeline/crypto_pipeline_dbt",
                  target="/usr/app/dbt", type="bind"),
            Mount(source="/c/Users/Admin/Documents/dev/crypto-pipeline/crypto_pipeline_dbt/profiles_docker",
                  target="/root/.dbt", type="bind"),
        ],
        auto_remove="success",
    )

    load_task = load_raw(extract_markets())
    load_task >> run_dbt


crypto_market_elt()