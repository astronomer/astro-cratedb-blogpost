"""
## Dynamically map task groups to create an ETL pipeline with CrateDB

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

from airflow.decorators import dag, task_group, task
from airflow.models.baseoperator import chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator
from airflow.utils.dates import datetime
import pandas as pd
import requests

CRATE_DB_CONN_ID = "cratedb_connection"
GH_API_URL = "https://api.github.com/repos/astronomer/learn-tutorials-data"
GH_CONTENT_URL = "https://raw.githubusercontent.com/astronomer/learn-tutorials-data"
GH_FOLDER_PATH = "/possum_partial"


@task
def get_file_names(base_url, folder_path):
    "Get the names of all files in a folder in a GitHub repo."
    folder_url = base_url + "/contents" + folder_path
    response = requests.get(folder_url)
    files = response.json()
    file_names = [file["name"] for file in files]
    return file_names


@task
def extract_data(base_url, folder_path, file_name):
    """Extract the contents of a CSV file in a GitHub repo
    and return a pandas DataFrame."""
    file_url = base_url + "/main" + folder_path + f"/{file_name}"
    possum_data = pd.read_csv(file_url)
    return possum_data


@task
def transform_data(dataset):
    """Transform the data by dropping rows with missing values.
    Return a tuple of lists of column values."""
    possum_final = dataset.dropna()
    return tuple(possum_final.to_dict(orient="list").values())


@dag(
    description="ETL Pipeline demo",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    template_searchpath=["include/"],
)
def astro_cratedb_elt():
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=CRATE_DB_CONN_ID,
        sql="sql/create_table.sql",
    )

    file_names = get_file_names(base_url=GH_API_URL, folder_path=GH_FOLDER_PATH)
    create_table >> file_names

    @task_group
    def extract_to_load(base_url, folder_path, file_name):
        """Extract data from a CSV file in a GitHub repo, transform it and
        load it into CrateDB."""

        extracted_data = extract_data(
            base_url=base_url, folder_path=folder_path, file_name=file_name
        )

        transformed_data = transform_data(dataset=extracted_data)

        SQLExecuteQueryOperator(
            task_id="load_data",
            conn_id=CRATE_DB_CONN_ID,
            sql="sql/insert_data.sql",
            parameters=transformed_data,
        )

    # dynamically map the task group over the list of file names, creating
    # one task group for each file
    extract_to_load_tg = extract_to_load.partial(
        base_url=GH_CONTENT_URL, folder_path=GH_FOLDER_PATH
    ).expand(file_name=file_names)

    data_check = SQLColumnCheckOperator(
        task_id="data_check",
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
        """Print the results of the upstream query to the logs
        in a pretty format."""
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
        extract_to_load_tg,
        data_check,
        select_data,
        print_selected_data(),
    )


astro_cratedb_elt()
