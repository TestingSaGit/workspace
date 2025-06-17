from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class ManageTablesOperator(BaseOperator):
    ui_color = '#80BD9E'


    @apply_defaults
    def __init__(self,
                 conn_id='',
                 delete_table_list=None,
                 create_table_list=None,
                 *args, **kwargs):

        super(ManageTablesOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.delete_table_list = delete_table_list or []
        self.create_table_list = create_table_list or []

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.conn_id)

        #drop tables
        for sql in self.delete_table_list:
            redshift.run(sql)
        #create tables
        for sql in self.create_table_list:
            self.log.info(f"creating: {sql}")
            redshift.run(sql)