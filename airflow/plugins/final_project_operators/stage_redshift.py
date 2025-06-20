from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend
from airflow.models import Variable

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'


    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        TRUNCATECOLUMNS
        BLANKSASNULL 
        EMPTYASNULL
        JSON '{}' 
    """


    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                aws_credentials_id="",
                table="",
                s3_bucket="",
                s3_key=None,
                json_format ="auto",
                file_date=None,
                *args, 
                **kwargs
                ):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        #params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_format = json_format
        self.file_date = file_date

    def execute(self, context):
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        redshift.run("DELETE FROM {}".format(self.table))
        

        s3_prefix = self.s3_key
        if self.file_date:
            year = str(self.file_date.strftime("%Y"))
            month = str(self.file_date.strftime("%m"))
            s3_prefix = s3_prefix.format(year, month)
        s3_path = """s3://{}/{}""".format(self.s3_bucket, s3_prefix)


        formated_statement = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            aws_connection.login,
            aws_connection.password,
            self.json_format
        )
        formated_statement = formated_statement.replace("\n", "")
        self.log.info(f'sql run: {formated_statement}')
        redshift.run(formated_statement)





