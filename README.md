Overview
========

Welcome! This repository contains a sample ETL pipeline using [Apache Airflow](https://airflow.apache.org/) with [CrateDB](https://crate.io/).

Project Contents
================

This Astro project was created using the [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli) and contains the following files and folders:

- .astro: This folder contains configuration files. We recommend to not modify these files.
- dags: This folder contains the Python files for your Airflow DAGs. By default, this directory includes two example DAGs:
    - `etl_pipeline`: This DAG shows an ETL pipeline example using CrateDB and a SQL check operator.
    - `test_cratedb`: This is a testing DAG running a query against your CrateDB connection.
- include/sql: This folder contains the SQL statements used by the `etl_pipeline` DAG.
- plugins: Add custom or community plugins for your project to this file. It is empty by default.
- tests: This folder contains the Python files for your Airflow tests. There are a couple of example tests included in this folder.
- .astro-registry.yaml: This file is used to add DAGs to the Astronomer registry. You can ignore it.
- .dockerignore: This file tells Docker which files to ignore when building your Docker image, see [.dockerignore file](https://docs.docker.com/engine/reference/builder/#dockerignore-file).
- .gitignore: This file tells Git which files to ignore when committing changes, see [.gitignore file](https://git-scm.com/docs/gitignore).
- Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience.
- packages.txt: Install OS-level packages needed for your project by adding them to this file. It is empty by default.
- requirements.txt: Install Python packages needed for your project by adding them to this file. In this project we install `pandas` and the [Postgres Airflow provider](https://registry.astronomer.io/providers/apache-airflow-providers-postgres/versions/latest) and pin their version.


Run this project locally
===========================

1. Clone this repository to your local machine.

2. Set a connection to your CrateDB database as an environment variable. You can do that either in the Dockerfile or by creating a file called `.env` in this project and adding:

```text
AIRFLOW_CONN_CRATEDB_CONNECTION = "postgres://<your-user>:<your-password>@<your-host>:<your-port>/?sslmode=<your-ssl-mode>"
```

3. Make sure you have the [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli) installed.

4. Start Airflow on your local machine by running 'astro dev start'.

This command will spin up 4 Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Webserver: The Airflow component responsible for rendering the Airflow UI
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- Triggerer: The Airflow component responsible for triggering deferred tasks

5. Verify that all 4 Docker containers were created by running 'docker ps'.

Note: Running 'astro dev start' will start your project with the Airflow Webserver exposed at port 8080 and Postgres exposed at port 5432. If you already have either of those ports allocated, you can either [stop your existing Docker containers or change the port](https://docs.astronomer.io/astro/test-and-troubleshoot-locally#ports-are-not-available).

6. Access the Airflow UI for your local Airflow project. To do so, go to http://localhost:8080/ and log in with 'admin' for both your Username and Password.

7. Run both DAGs manually by clicking on the Play buttons. You should see the DAGs running successfully.

Deploy this project to Astronomer
=================================

If you have an Astronomer account, pushing code to a Deployment on Astronomer is simple. For deploying instructions, refer to Astronomer documentation: https://docs.astronomer.io/cloud/deploy-code/

Contact
=======

The project was created with :heart: by the DevRel teams of CrateDB and Astronomer. To report bugs feel free to open an issue on this repository.
If you want to get in touch with us, you can reach out to us in the respective communities:

- [CrateDB Community](https://community.crate.io/)
- [Airflow Slack](https://apache-airflow-slack.herokuapp.com/) (join the channel #airflow-astronomer for Astronomer specific questions and regular content updates).

Resources
=========

- [Astronomer Learn](https://docs.astronomer.io/learn) (to learn how the `load_data` task works check out our guide on [dynamic task mapping](https://docs.astronomer.io/learn/dynamic-tasks).)
- [Astro documentation](https://docs.astronomer.io/astro)
- [Astronomer Academy](https://academy.astronomer.io/)
- [Astronomer webinars](https://www.astronomer.io/events/webinars/)
- [CrateDB documentation](https://crate.io/docs/crate/reference/en/5.3/)
- [CrateDB Cloud tutorials](https://crate.io/blog/tag/cratedb-cloud)
- [CrateDB webinars](https://crate.io/resources/webinars)