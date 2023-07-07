from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from pendulum import datetime 


@dag(
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
)
def test_cratedb():

    f = PostgresOperator(
        task_id="test",
        database="dev",
        sql="SELECT name FROM sys.cluster",
        postgres_conn_id="cratedb_connection",
    )

test_cratedb()