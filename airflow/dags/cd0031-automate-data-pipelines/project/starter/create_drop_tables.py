from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.manage_tables import ManageTablesOperator
from udacity.common.final_project_sql_statements import SqlQueries


default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
}

@dag(
    default_args=default_args,
    description='Manage tables creation and deletion',
    schedule_interval='0 * * * *'
)
def create_drop_tables():
    sql = SqlQueries()

    start_operator = DummyOperator(task_id='Begin_execution')
    end_operator = DummyOperator(task_id='End_execution')

    create_drop_task = ManageTablesOperator(
        task_id='create_table',
        conn_id="redshift",
        delete_table_list=sql.drop_table_list,
        create_table_list=sql.create_table_list
    )
    start_operator >> create_drop_task >> end_operator

create_drop_tables_dag = create_drop_tables()