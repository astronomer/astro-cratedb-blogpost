import os
import pandas as pd

from airflow.utils.dates import datetime
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator

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
def etl_pipeline():
    create_table = SQLExecuteQueryOperator(
        task_id="create_possum_table",
        conn_id=CRATE_DB_CONN_ID,
        sql="sql/create_table.sql",
    )

    extract = extract_data()

    transform = transform_data(extract)

    load_data = SQLExecuteQueryOperator.partial(
        task_id="load_possum_data",
        conn_id=CRATE_DB_CONN_ID,
        sql="sql/insert_data.sql",
    ).expand(parameters=transform)

    select_data = SQLExecuteQueryOperator(
        task_id="select_data",
        conn_id=CRATE_DB_CONN_ID,
        sql="sql/select.sql",
    )

    data_check = SQLColumnCheckOperator(
        task_id="value_check",
        conn_id=CRATE_DB_CONN_ID,
        table="doc.possum",
        column_mapping={
            "age": {"min": {"geq_to": 0}},
        },
    )

etl_pipeline()
    chain(create_table, extract, transform, load_data, data_check, select_data)
