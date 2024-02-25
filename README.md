# Data Pipelines with Airflow

## Project Overview

The objective of this project is to enhance the data warehouse ETL pipelines for Sparkify, a music streaming company, by utilizing the capabilities of Apache Airflow. The goal is to create high-grade, dynamic data pipelines composed of reusable tasks, enabling easy monitoring, and supporting backfilling. The project also emphasizes the importance of data quality by executing tests against datasets after ETL steps to identify and rectify discrepancies.

### Project Requirements

- **Automation and Monitoring**: Introduce automation and monitoring to the existing ETL pipelines using Apache Airflow.
  
- **Dynamic and Reusable Tasks**: Create dynamic data pipelines with tasks that are reusable, allowing for flexibility and efficiency in the pipeline design.

- **Monitoring and Backfills**: Implement monitoring capabilities for the data pipelines and ensure easy backfilling of data when necessary.

- **Data Quality Checks**: Run tests against datasets post-ETL to ensure data quality, identifying and addressing any discrepancies that may arise.

### Project Architecture

The source data resides in S3, and the processing is to be done in Sparkify's data warehouse in Amazon Redshift. The source datasets include JSON logs detailing user activity and JSON metadata about the songs users listen to.

### Getting Started

1. **Clone the Repository**: Clone the project repository to your local environment.

    ```bash
    git clone [repository_url]
    ```

2. Initiating the Airflow Web Server
Ensure [Docker Desktop](https://www.docker.com/products/docker-desktop/) is installed before proceeding.
To bring up the entire app stack up, we use [docker-compose](https://docs.docker.com/engine/reference/commandline/compose_up/) as shown below

    ```bash
    docker-compose up -d
    ```
    
    Visit http://localhost:8080 once all containers are up and running.

3. Configuring Connections in the Airflow Web Server UI
![Airflow Web Server UI. Credentials: `airflow`/`airflow`.](assets/login.png)

    On the Airflow web server UI, use `airflow` for both username and password.
    * Post-login, navigate to **Admin > Connections** to add required connections - specifically, `aws_credentials` and `redshift`.
    * Don't forget to start your Redshift cluster via the AWS console.
    * After completing these steps, run your DAG to ensure all tasks are successfully executed.

### Project Structure

- **dags/**: Contains the Directed Acyclic Graphs (DAGs) defining the workflow.
  
- **plugins/**: Houses custom operators and helpers necessary for the pipeline tasks.

- **helpers/**: Includes the helpers class with SQL transformations for ETL processes.

### Project Execution

1. Create Tables on Redshift

    1. Go to AWS console and search `Amazon Redshift`
    2. Go to `Query Editor`
    3. Copy and paste all SQL statements in the file, `create_tables.sql` in this repository
    4. Run the query
    5. Check if 8 tables are created in your database, `dev`

2. Run DAG

    1. Open your `airflow webserver UI` and log in
    2. Go to `DAGs` and find the dag named `final_project`
    3. Click play button to start the dag

### Additional Information
**\<DAG graph image of this project\>**

![Working DAG with correct task dependencies](assets/final_project_dag_graph2.png)
