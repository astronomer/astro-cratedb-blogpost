"""
## DAG to test your CrateDB connection

Define the connection to CrateDB as an environment variable in the format:
AIRFLOW_CONN_CRATEDB_CONNECTION = "postgres://<your-user>:<your-password>@<your-host>:<your-port>/?sslmode=<your-ssl-mode>"
"""

from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from pendulum import datetime


CRATE_DB_CONN_ID = "cratedb_connection"


@dag(
    start_date=datetime(2023, 7, 1),
    schedule=None,
    catchup=False,
)
def test_cratedb():
    PostgresOperator(
        task_id="test_connection",
        database="dev",
        sql="SELECT name FROM sys.cluster",
        postgres_conn_id=CRATE_DB_CONN_ID,
    )


test_cratedb()
