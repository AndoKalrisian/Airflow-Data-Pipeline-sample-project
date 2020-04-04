from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 target_table="",
                 sql_table_create="",
                 sql_table_insert="",
                 redshift_conn_id="",
                 mode="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)

        self.target_table=target_table
        self.sql_table_create=sql_table_create
        self.sql_table_insert=sql_table_insert
        self.redshift_conn_id=redshift_conn_id
        self.mode=mode

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.mode == 'append':
            redshift.run(f"INSERT INTO {self.target_table} {self.sql_table_insert}")
        else:   
            self.log.info('Dropping table if exists')
            redshift.run(f'DROP TABLE IF EXISTS {self.target_table}')
            
            self.log.info('Creating fact table if not exists')
            redshift.run(self.sql_table_create)
            
            self.log.info('Inserting data into fact table')
            redshift.run(f"INSERT INTO {self.target_table} {self.sql_table_insert}")
