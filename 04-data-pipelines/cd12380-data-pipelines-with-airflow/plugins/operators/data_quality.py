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
                  sql_queries=[],
                  *args, **kwargs):
        """Initialize the DataQualityOperator, inheriting from BaseOperator.

        Args:
            redshift_conn_id (str): The redshift connection id for redshift staging table
            sql_queries (SqlDataQualityQueries): The abstract class SqlDataQualityQueries
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_queries = sql_queries
        
    
    def __run_data_quality_checks(self):
        """Run data quality checks
        
          Returns:
              ValueError: If data quality checks fail
        """
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        # Check for empty tables
        for query in self.sql_queries.count_rows:
            records = redshift_hook.get_records(query)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {query} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {query} contained 0 rows")
            self.log.info(f"Data quality check passed. {query} returned {records[0][0]} records")
        
        # Check for null values
        for query in self.sql_queries.check_nulls:
            records = redshift_hook.get_records(query)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {query} returned no results")
            num_records = records[0][0]
            if num_records > 0:
                raise ValueError(f"Data quality check failed. {query} contained {records[0][0]} null values")
            self.log.info(f"Data quality check passed. {query} returned 0 null values")
        
        # Check for duplicates
        for query in self.sql_queries.check_duplicates:
            records = redshift_hook.get_records(query)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {query} returned no results")
            num_records = records[0][0]
            if num_records > 0:
                raise ValueError(f"Data quality check failed. {query} contained {records[0][0]} duplicates")
            self.log.info(f"Data quality check passed. {query} returned 0 duplicates")
            
            

    def execute(self, context):
        """Execute the operator to run data quality checks
        """
        self.log.info('DataQualityOperator not implemented yet')
        
        # Run data quality checks
        self.__run_data_quality_checks()