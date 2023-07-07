import os
from airflow import DAG
from airflow.utils.dates import datetime
from airflow.decorators import task
from airflow.models.baseoperator import chain

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator

import pandas as pd
import seaborn as sns

CREATE_DB_CONN_ID = "cratedb_connection"


@task
def extract_data():
    # here we can use other datasets, or load from S3 if you have some interesting example
    tips = sns.load_dataset("tips")
    return tips


# remove rows with empty column and add a new column total_paid
@task
def transform_data(dataset):
    tips_final = dataset.dropna()
    tips_final["total_paid"] = tips_final["total_bill"] + tips_final["tip"]
    print(tips_final.head(5).values.tolist())
    return tips_final.values.tolist()


with DAG(
    "etl-pipeline",
    description="ETL Pipeline demo",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    template_searchpath=["include/"],
) as dag:
    create_table = SQLExecuteQueryOperator(
        task_id="create_tips_table",
        conn_id=CREATE_DB_CONN_ID,
        sql="sql/create_table.sql",
    )

    extract = extract_data()

    transform = transform_data(extract)

    load_data = SQLExecuteQueryOperator.partial(
        task_id="load_tips_data",
        conn_id=CREATE_DB_CONN_ID,
        sql="sql/insert_data.sql",
    ).expand(parameters=transform)

    select_data = SQLExecuteQueryOperator(
        task_id="select_data",
        conn_id=CREATE_DB_CONN_ID,
        sql="SELECT AVG(total_paid) FROM doc.tips",
    )

    data_check = SQLColumnCheckOperator(
        task_id="value_check",
        conn_id=CREATE_DB_CONN_ID,
        table="doc.tips",
        column_mapping={
            "total_paid": {"min": {"geq_to": 0}},
        },
    )

    chain(create_table, extract, transform, load_data, data_check, select_data)
