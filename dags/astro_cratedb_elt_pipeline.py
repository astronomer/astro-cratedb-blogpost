"""
## Run an ETL pipeline with CrateDB and data quality checks

This DAG runs an ETL pipeline using the [CrateDB](https://crate.io/) database.
The DAG will:
- create a table in CrateDB
- extract data from a CSV file in a public GH repository
- transform the data in memory
- load the data into CrateDB
- run a data quality check on the data in CrateDB 
- query the data in CrateDB
- use XCom to print the query results as a log statement

Define the connection to CrateDB as an environment variable in the format:
AIRFLOW_CONN_CRATEDB_CONNECTION = "postgres://<your-user>:<your-password>@<your-host>:<your-port>/?sslmode=<your-ssl-mode>"
"""

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator
from airflow.utils.dates import datetime
import pandas as pd

CRATE_DB_CONN_ID = "cratedb_connection"


@task
def extract_data():
    url = "https://raw.githubusercontent.com/astronomer/learn-tutorials-data/main/possum.csv"
    possum = pd.read_csv(url)
    return possum


# remove rows with empty column
@task
def transform_data(dataset):
    possum_final = dataset.dropna()
    return possum_final.values.tolist()


@dag(
    description="ETL Pipeline demo",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    template_searchpath=["include/"],
)
def astro_cratedb_elt_pipeline():
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=CRATE_DB_CONN_ID,
        sql="sql/create_table.sql",
    )

    extract = extract_data()

    transform = transform_data(extract)

    load_data = SQLExecuteQueryOperator.partial(
        task_id="load_data",
        conn_id=CRATE_DB_CONN_ID,
        sql="sql/insert_data.sql",
    ).expand(parameters=transform)

    data_check = SQLColumnCheckOperator(
        task_id="value_check",
        conn_id=CRATE_DB_CONN_ID,
        table="doc.possum",
        column_mapping={
            "age": {"min": {"geq_to": 0}},
        },
    )

    select_data = SQLExecuteQueryOperator(
        task_id="select_data",
        conn_id=CRATE_DB_CONN_ID,
        sql="sql/select.sql",
    )

    @task
    def print_selected_data(**context):
        selected_data = context["ti"].xcom_pull(task_ids="select_data")
        for i in selected_data:
            possum_sex = "Female" if i[0] == "f" else "Male"
            possum_pop = (
                "Victoria" if i[1] == "Vic" else "New South Wales and Queensland"
            )
            tail_l = i[2]
            print(
                f"{possum_sex} possums of the {possum_pop} population had an average tail length of {tail_l} cm."
            )

    chain(
        create_table,
        extract,
        transform,
        load_data,
        data_check,
        select_data,
        print_selected_data(),
    )


astro_cratedb_elt_pipeline()
