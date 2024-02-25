# Data Pipelines with Airflow

Welcome to the Data Pipelines with Airflow project! This endeavor will provide you with a solid understanding of Apache Airflow's core concepts. Your task involves creating custom operators to execute essential functions like staging data, populating a data warehouse, and validating data through the pipeline.

To begin, we've equipped you with a project template that streamlines imports and includes four unimplemented operators. These operators need your attention to turn them into functional components of a data pipeline. The template also outlines tasks that must be interconnected for a coherent and logical data flow.

A helper class containing all necessary SQL transformations is at your disposal. While you won't have to write the ETL processes, your responsibility lies in executing them using your custom operators.

## Initiating the Airflow Web Server
Ensure [Docker Desktop](https://www.docker.com/products/docker-desktop/) is installed before proceeding.

To bring up the entire app stack up, we use [docker-compose](https://docs.docker.com/engine/reference/commandline/compose_up/) as shown below

```bash
docker-compose up -d
```
Visit http://localhost:8080 once all containers are up and running.

## Configuring Connections in the Airflow Web Server UI
![Airflow Web Server UI. Credentials: `airflow`/`airflow`.](assets/login.png)

On the Airflow web server UI, use `airflow` for both username and password.
* Post-login, navigate to **Admin > Connections** to add required connections - specifically, `aws_credentials` and `redshift`.
* Don't forget to start your Redshift cluster via the AWS console.
* After completing these steps, run your DAG to ensure all tasks are successfully executed.

## Create Tables on Redshift

1. Go to AWS console and search `Amazon Redshift`
2. Go to `Query Editor`
3. Copy and paste all SQL statements in the file, `crete_tables.sql` in this repository
4. Run the query
5. Check if 8 tables are created in your database, `dev`

## Run DAG

1. Open your `airflow webserver UI` and log in
2. Go to `DAGs` and find the dag named `final_project`
3. Click play button to start the dag

## DAG graph
![Working DAG with correct task dependencies](assets/final_project_dag_graph2.png)
