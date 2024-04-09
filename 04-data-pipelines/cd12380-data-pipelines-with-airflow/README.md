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

- Post-login, navigate to **Admin > Connections** to add required connections - specifically, `aws_credentials` and `redshift`.
- Don't forget to start your Redshift cluster via the AWS console.
- After completing these steps, run your DAG to ensure all tasks are successfully executed.

## Getting Started with the Project

1. The project template package comprises three key components:

   - The **DAG template** includes imports and task templates but lacks task dependencies.
   - The **operators** folder with operator templates.
   - A **helper class** for SQL transformations.

1. With these template files, you should see the new DAG in the Airflow UI, with a graph view resembling the screenshot below:
   ![Project DAG in the Airflow UI](assets/final_project_dag_graph1.png)
   You should be able to execute the DAG successfully, but if you check the logs, you will see only `operator not implemented` messages.

## DAG Configuration

In the DAG, add `default parameters` based on these guidelines:

- No dependencies on past runs.
- Tasks are retried three times on failure.
- Retries occur every five minutes.
- Catchup is turned off.
- No email on retry.

Additionally, configure task dependencies to match the flow depicted in the image below:
![Working DAG with correct task dependencies](assets/final_project_dag_graph2.png)

## Developing Operators

To complete the project, build four operators for staging data, transforming data, and performing data quality checks. While you can reuse code from Project 2, leverage Airflow's built-in functionalities like connections and hooks whenever possible to let Airflow handle the heavy lifting.

### Stage Operator

Load any JSON-formatted files from S3 to Amazon Redshift using the stage operator. The operator should create and run a SQL COPY statement based on provided parameters, distinguishing between JSON files. It should also support loading timestamped files from S3 based on execution time for backfills.

### Fact and Dimension Operators

Utilize the provided SQL helper class for data transformations. These operators take a SQL statement, target database, and optional target table as input. For dimension loads, implement the truncate-insert pattern, allowing for switching between insert modes. Fact tables should support append-only functionality.

### Data Quality Operator

Create the data quality operator to run checks on the data using SQL-based test cases and expected results. The operator should raise an exception and initiate task retry and eventual failure if test results don't match expectations.

## Reviewing Starter Code

Before diving into development, familiarize yourself with the following files:

- [plugins/operators/data_quality.py](plugins/operators/data_quality.py)
- [plugins/operators/load_fact.py](plugins/operators/load_fact.py)
- [plugins/operators/load_dimension.py](plugins/operators/load_dimension.py)
- [plugins/operators/stage_redshift.py](plugins/operators/stage_redshift.py)
- [plugins/helpers/sql_queries.py](plugins/helpers/sql_queries.py)
- [dags/final_project.py](dags/final_project.py)

Now you're ready to embark on this exciting journey into the world of Data Pipelines with Airflow!

## Project Rubric

### General

- The dag and plugins do not give an error when imported to Airflow

  - DAG can be browsed without issues in the Airflow UI
    ![DAG in Airflow UI](assets/The%20dag%20and%20plugins%20do%20not%20give%20an%20error%20when%20imported%20to%20Airflow.png)

- All tasks have correct dependencies

  - The dag follows the data flow provided in the instructions, all the tasks have a dependency and DAG begins with a start_execution task and ends with a end_execution task.
    ![All tasks have correct dependencies](assets/All%20tasks%20have%20correct%20dependencies.png)

### Data Configuration

- Default_args object is used in the DAG

  - DAG contains default_args dictionary, with the following keys:
    - Owner
    - Depends_on_past
    - Start_date
    - Retries
    - Retry_delay
    - Catchup
      ![Default_args object is used in the DAG](assets/Default_args%20object%20is%20used%20in%20the%20DAG.png)

- Defaults_args are bind to the DAG

  - The DAG object has default args set
    ![Default_args object is used in the DAG](assets/Defaults_args%20are%20bind%20to%20the%20DAG.png)

- The DAG has a correct schedule
  - The DAG should be scheduled to run once an hour
    ![The DAG has a correct schedule](assets/The%20DAG%20has%20a%20correct%20schedule.png)

[Data Configuration](./dags/final_project.py)

### Staging the data

- Task to stage JSON data is included in the DAG and uses the RedshiftStage operator

  - There is a task that to stages data from S3 to Redshift. (Runs a Redshift copy statement)
    ![Task to stage JSON data is included in the DAG and uses the RedshiftStage operator](assets/Task%20to%20stage%20JSON%20data%20is%20included%20in%20the%20DAG.png)

- Task uses params

  - Instead of running a static SQL statement to stage the data, the task uses params to generate the copy statement dynamically
    ![Task uses params](assets/Task%20uses%20params.png)

- Logging used

  - The operator contains logging in different steps of the execution
    ![Logging used](assets/Logging%20used.png)

- The database connection is created by using a hook and a connection
  - The SQL statements are executed by using a Airflow hook
    ![The database connection is created by using a hook and a connection](assets/The%20database%20connection%20is%20created%20by%20using%20a%20hook%20and%20a%20connection.png)

[Staging the data](./plugins/operators/stage_redshift.py)

### Loading dimensions and facts

- Set of tasks using the dimension load operator is in the DAG

  - Dimensions are loaded with on the LoadDimension operator
    ![Set of tasks using the dimension load operator is in the DAG](assets/Set%20of%20tasks%20using%20the%20dimension%20load%20operator%20is%20in%20the%20DAG.png)

- A task using the fact load operator is in the DAG

  - Facts are loaded with on the LoadFact operator
    ![A task using the fact load operator is in the DAG](assets/A%20task%20using%20the%20fact%20load%20operator%20is%20in%20the%20DAG.png)

- Both operators use params

  - Instead of running a static SQL statement to stage the data, the task uses params to generate the copy statement dynamically
    ![Both operators use params](assets/Both%20operators%20use%20params.png)

- The dimension task contains a param to allow switch between append and insert-delete functionality

  - The DAG allows to switch between append-only and delete-load functionality
    ![The dimension task contains a param to allow switch between append and insert-delete functionality](assets/The%20dimension%20task%20contains%20a%20param%20to%20allow%20switch%20between%20append%20and%20insert-delete%20functionality.png)

[Loading dimensions](./plugins/operators/load_dimension.py)
[Loading facts](./plugins/operators/load_fact.py)

### Data Quality Checks

- A task using the data quality operator is in the DAG and at least one data quality check is done

  - Data quality check is done with correct operator
    ![A task using the data quality operator](assets/A%20task%20using%20the%20data%20quality%20operator%20is%20in%20the%20DAG.png)

- The operator raises an error if the check fails pass

  - The DAG either fails or retries n times
    ![The operator raises an error if the check fails pass](assets/The%20operator%20raises%20an%20error%20if%20the%20check%20fails%20pass.png)

- The operator is parametrized

  - Operator uses params to get the tests and the results, tests are not hard coded to the operator
    ![The operator is parametrized](assets/The%20operator%20is%20parametrized.png)

[Data Quality Checks](./plugins/operators/data_quality.py)
[SQL Helper](./plugins/helpers/sql_data_quality_checks_queries.py)
