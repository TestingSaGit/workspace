from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 table="",
                 sql="",
                 append=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.append=append
        self.table=table

    def execute(self, context):
        self.log.info(f'LoadDimensionOperator: {self.sql}')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        #delete data when table name distinct to "" and append is False
        if self.table != "" and not self.append:
            self.log.info("Delete data from table: {}".format(self.table))
            redshift_hook.run("DELETE FROM {}".format(self.table))

        redshift_hook.run(self.sql)

