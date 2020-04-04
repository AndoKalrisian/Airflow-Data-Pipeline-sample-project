from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    @apply_defaults
    def __init__(self,
                 target_table="",
                 sql_table_create="",
                 redshift_conn_id="",
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_key="",
                 json_file="",
                 region="",
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.target_table = target_table
        self.sql_table_create = sql_table_create
        self.json_file = json_file
        self.region = region

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        aws = AwsHook(self.aws_credentials_id)
        credentials = aws.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        self.log.info('Dropping table if exists')
        redshift.run(f'DROP TABLE IF EXISTS {self.target_table}')
        
        self.log.info(f"Creating {self.target_table} if not exists")
        redshift.run(self.sql_table_create)
        
        self.log.info(f"Clearing redshift table {self.target_table}")
        redshift.run(f"DELETE FROM {self.target_table}")
        
        self.log.info("Copying data from S3 to Redshift")
        redshift.run(("""
                     COPY {}
                     FROM '{}'
                     WITH CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}'
                     JSON '{}'
                     REGION '{}'
                     """
                     ).format(
                         self.target_table,
                         s3_path,
                         credentials.access_key,
                         credentials.secret_key,
                         self.json_file,
                         self.region
                     ))




