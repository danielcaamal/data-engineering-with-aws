from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Operator to load data into fact table in redshift
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__( 
                  self,
                  redshift_conn_id="",
                  table="",
                  sql_insert="",
                  insert_mode="append",
                  *args, **kwargs):
        """Initialize the LoadFactOperator, inheriting from BaseOperator
        
        Args:
            redshift_conn_id (str): The redshift connection id for redshift staging table
            table (str): The target table name in redshift
            sql_insert (str): The sql select statement to insert data into the fact table
            insert_mode (str): The insert mode, either 'replace' or 'append'
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_insert = sql_insert
        self.insert_mode = insert_mode
        self.__validate_params()
        
    def __validate_params(self):
        """Validate the input parameters
        
        Returns:
            ValueError: If the redshift connection id is not provided
            ValueError: If the table name is not provided
            ValueError: If the sql insert is not provided
            ValueError: If the insert mode is not 'replace' or 'append'
        """
        if not self.redshift_conn_id:
            raise ValueError('The redshift_conn_id is required')
        if not self.table:
            raise ValueError('The table is required')
        if not self.sql_insert:
            raise ValueError('The sql insert is required')
        if self.insert_mode not in ["replace", "append"]:
            raise ValueError('The insert mode should be either "replace" or "append"')

    def __get_redshift_hook(self):
        """Get the redshift hook instance

        Returns:
            PostgresHook: The redshift hook instance
        """
        return PostgresHook(postgres_conn_id=self.redshift_conn_id)
    
        
    def __load_data_into_fact_table(self, redshift):
        """Load the data into the fact table, only append mode is supported
        
        Args:
            redshift (PostgresHook): The redshift hook instance
        """
        self.log.info(f"Loading data into fact table {self.table} in redshift")
        
        # It should be append only
        # if self.insert_mode == "replace":
        #   self.log.info(f"Deleting data from {self.table}")
        #   redshift.run(f"DELETE FROM {self.table}")
        #   self.log.info(f"Inserting data into {self.table}")
        
        redshift.run(f"""INSERT INTO {self.table} {self.sql_insert} ;""")
        self.log.info("Data loaded into fact table in redshift")
    
    def execute(self, context):
        """Execute the operator to load data into the fact table
        """
        self.log.info('Starting the LoadFactOperator')
        
        # Get the redshift hook instance
        redshift = self.__get_redshift_hook()
        
        # Load the data from staging tables into the fact table
        self.__load_data_into_fact_table(redshift)
