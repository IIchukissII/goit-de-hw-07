from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.sensors.sql_sensor import SqlSensor
from airflow.utils.dates import days_ago
import random
import time

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

# Define the DAG
with DAG(
    "medal_count_dag",
    default_args=default_args,
    description="DAG to count medals and insert into MySQL table",
    schedule_interval=None,
) as dag:

    def pick_medal():
        """
        Randomly selects a medal type (Bronze, Silver, or Gold) and returns it.
        """
        medal = random.choice(["Bronze", "Silver", "Gold"])
        return medal

    def branch_func(**kwargs):
        """
        Determines the branch to execute based on the medal type picked by the `pick_medal` task.

        Returns:
            str: Task ID of the branch to execute (calc_Bronze, calc_Silver, or calc_Gold).
        """
        ti = kwargs["ti"]
        medal = ti.xcom_pull(task_ids="pick_medal")
        return f"calc_{medal}"

    def generate_delay():
        """
        Introduces a delay to simulate a wait period before proceeding to the next task.
        """
        time.sleep(1)  

    # Task: Create the table if it doesn't exist
    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id="goit_mysql_connection",
        sql="""
        CREATE TABLE IF NOT EXISTS romanchuk_medal_counts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at DATETIME
        );
        """,
    )

    # Task: Randomly select a medal type
    pick_medal_task = PythonOperator(
        task_id="pick_medal",
        python_callable=pick_medal,
    )

    # Task: Determine which branch to follow
    branching = BranchPythonOperator(
        task_id="branching",
        python_callable=branch_func,
        provide_context=True,
    )

    # Tasks: Count medals and insert into the table
    calc_Bronze = MySqlOperator(
        task_id="calc_Bronze",
        mysql_conn_id="goit_mysql_connection",
        sql="""
        INSERT INTO romanchuk_medal_counts (medal_type, count, created_at)
        SELECT 'Bronze', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze';
        """,
    )

    calc_Silver = MySqlOperator(
        task_id="calc_Silver",
        mysql_conn_id="goit_mysql_connection",
        sql="""
        INSERT INTO romanchuk_medal_counts (medal_type, count, created_at)
        SELECT 'Silver', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
        """,
    )

    calc_Gold = MySqlOperator(
        task_id="calc_Gold",
        mysql_conn_id="goit_mysql_connection",
        sql="""
        INSERT INTO romanchuk_medal_counts (medal_type, count, created_at)
        SELECT 'Gold', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
        """,
    )

    # Task: Introduce a delay
    generate_delay_task = PythonOperator(
        task_id="generate_delay",
        python_callable=generate_delay,
        trigger_rule="none_failed_min_one_success",
    )

    # Task: Check if the newest record is not older than 30 seconds
    check_for_correctness = SqlSensor(
        task_id="check_for_correctness",
        conn_id="goit_mysql_connection",
        sql="""
        SELECT 1 FROM romanchuk_medal_counts
        WHERE created_at >= NOW() - INTERVAL 30 SECOND
        ORDER BY created_at DESC
        LIMIT 1;
        """,
        poke_interval=5,
        timeout=60,
    )

    # Define task dependencies
    create_table >> pick_medal_task >> branching
    branching >> [calc_Bronze, calc_Silver, calc_Gold]
    (
        [calc_Bronze, calc_Silver, calc_Gold]
        >> generate_delay_task
        >> check_for_correctness
    )
