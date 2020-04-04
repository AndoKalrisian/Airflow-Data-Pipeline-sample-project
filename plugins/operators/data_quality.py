from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 data_test_arr=[],
                 *args, **kwargs):
        
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self.log.info('DataQualityOperator init')
        
        self.redshift_conn_id=redshift_conn_id
        self.data_test_arr=data_test_arr

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for test in self.data_test_arr:
            self.log.info(f"DataTest with query {test['query']}")
            records = redshift.get_records(test['query'])

            # no records
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(
                    f"Data quality check failed. {test['query']} returned no results.")
                
            num_records = records[0][0]
            
            if test["operator"](num_records, test["test_value"]):
                self.log.info(f"Data Quality Checked PASSED: The query: {test['query']} returned the result of {test['test_value']}, {num_records} {test['operator']} {test['test_value']}")
            else:
                raise ValueError(f"Data quality check FAILED: {num_records} is not {test['operator']} {test['test_value']}")
                    
                    
        
            
            
