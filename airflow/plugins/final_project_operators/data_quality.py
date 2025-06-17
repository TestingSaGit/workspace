from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table


    def execute(self, context):
        self.log.info(f'Running data quality checks on table {self.table}')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        try:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table}")
        except Exception as e:
            raise ValueError(f"An error occurred querying Redshift: {e}")

        if not records or not records[0]:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")

        num_records = records[0][0]

        if num_records < 1:
            raise ValueError(f"Data quality check failed. {self.table} contained 0 records")

        self.log.info(f"Data quality on table {self.table} check passed with {num_records} records")
