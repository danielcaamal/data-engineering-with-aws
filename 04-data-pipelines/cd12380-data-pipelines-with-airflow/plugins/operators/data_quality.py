from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Operator to run data quality checks
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(
                  self,
                  redshift_conn_id="",
                  dq_checks=[],
                  *args, **kwargs):
        """Initialize the DataQualityOperator, inheriting from BaseOperator.

        Args:
            redshift_conn_id (str): The redshift connection id for redshift staging table
            sql_queries (SqlDataQualityQueries): The abstract class SqlDataQualityQueries
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks
        
    
    def __run_data_quality_checks(self):
        """Run data quality checks
        
          Returns:
              ValueError: If data quality checks fail
        """
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        # Check data_quality_checks
        # Example {'check_sql': "xxx", 'expected_result': xxx , comparison:'>'}]
        for check in self.dq_checks:
            check_sql = check.get('check_sql')
            expected_result = check.get('expected_result')
            comparison = check.get('comparison')
            
            self.log.info(f'Running data quality check: {check_sql}')
            records = redshift_hook.get_records(check_sql)
            
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {check_sql} returned no results")
            
            num_records = records[0][0]
            if comparison == '>':
                if num_records <= expected_result:
                    raise ValueError(f"Data quality check failed. {check_sql} returned {num_records} records, expected more than {expected_result}")
            elif comparison == '=':
                if num_records != expected_result:
                    raise ValueError(f"Data quality check failed. {check_sql} returned {num_records} records, expected {expected_result}")
            elif comparison == '<':
                if num_records >= expected_result:
                    raise ValueError(f"Data quality check failed. {check_sql} returned {num_records} records, expected less than {expected_result}")
            else:
                raise ValueError(f"Data quality check failed. Comparison operator {comparison} not supported")
            
            self.log.info(f"Data quality check passed. {check_sql} returned {num_records} records")

    def execute(self, context):
        """Execute the operator to run data quality checks
        """
        self.log.info('DataQualityOperator not implemented yet')
        
        # Run data quality checks
        self.__run_data_quality_checks()