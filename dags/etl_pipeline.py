import os
import pandas as pd

from airflow.utils.dates import datetime
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator

CRATE_DB_CONN_ID = "cratedb_connection"
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
key="possum/possum.csv"


@task
def extract_data():
    possum = pd.read_csv(   
    f"s3://{AWS_S3_BUCKET}/{key}",
    storage_options={
        "key": AWS_ACCESS_KEY_ID,
        "secret": AWS_SECRET_ACCESS_KEY
    })
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
        sql="SELECT AVG(age) FROM doc.possum",
    )

    data_check = SQLColumnCheckOperator(
        task_id="value_check",
        conn_id=CRATE_DB_CONN_ID,
        table="doc.possum",
        column_mapping={
            "total_paid": {"age": {"geq_to": 0}},
        },
    )

etl_pipeline()
    chain(create_table, extract, transform, load_data, data_check, select_data)
