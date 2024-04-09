from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__( 
                  self,
                  # Define your operators params (with defaults) here
                  # Example:
                  # conn_id = your-connection-name
                  redshift_conn_id="",
                  table="",
                  sql_insert="",
                  insert_mode="append",
                  *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_insert = sql_insert
        self.insert_mode = insert_mode
        self.__validate_params()
        
    def __validate_params(self):
        if not self.redshift_conn_id:
            raise ValueError('The redshift_conn_id is required')
        if not self.table:
            raise ValueError('The table is required')
        if not self.sql_insert:
            raise ValueError('The sql insert is required')
        if not self.insert_mode:
            raise ValueError('The insert mode is required')
        if self.insert_mode not in ["replace", "append"]:
            raise ValueError('The insert mode should be either "replace" or "append"')

    def __get_redshift_hook(self):
        return PostgresHook(postgres_conn_id=self.redshift_conn_id)
    
        
    def __load_data_into_fact_table(self, redshift):
        self.log.info("Loading data into fact table in redshift")
        if self.insert_mode == "replace":
          self.log.info(f"Deleting data from {self.table}")
          redshift.run(f"DELETE FROM {self.table}")
          self.log.info(f"Inserting data into {self.table}")
        
        redshift.run(f"""INSERT INTO {self.table} {self.sql_insert} ;""")
        self.log.info("Data loaded into fact table in redshift")
    
    def execute(self, context):
        self.log.info('Starting the LoadFactOperator')
        
        # Get the redshift hook instance
        redshift = self.__get_redshift_hook()
        
        # Load the data from staging tables into the fact table
        self.__load_data_into_fact_table(redshift)
