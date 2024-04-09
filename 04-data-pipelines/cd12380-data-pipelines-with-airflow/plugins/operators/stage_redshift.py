from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    Operator to stage data from S3 to Redshift staging table 
    using the copy command
    """
    
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'  
        {}
    """

    @apply_defaults
    def __init__(
                  self,
                  redshift_conn_id="",
                  aws_credentials_id="",
                  table="",
                  s3_bucket="",
                  s3_key="",
                  copy_params="",
                  context={},
                  *args, **kwargs):
        """Initialize the StageToRedshiftOperator, inheriting from BaseOperator.
        
        Args:
            redshift_conn_id (str): The redshift connection id for redshift staging table
            aws_credentials_id (str): The aws credentials id for s3
            table (str): The target table name in redshift
            s3_bucket (str): The s3 bucket name source
            s3_key (str): The s3 key source
            copy_params (str): The copy params extra
            context (dict): The context to be rendered in the s3 key
        """

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.context = context
        self.copy_params = copy_params
        self.__validate_params()

    def __validate_params(self):
        """Validate the input parameters
        
        Returns:
            ValueError: If the redshift connection id is not provided
            ValueError: If the aws credentials id is not provided
            ValueError: If the table name is not provided
            ValueError: If the s3 bucket name is not provided
            ValueError: If the s3 key is not provided
        """
        if not self.redshift_conn_id:
            assert ValueError('The redshift connection id must be provided')
        if not self.aws_credentials_id:
            assert ValueError('The aws credentials id must be provided')
        if not self.table:
            assert ValueError('The table name must be provided')
        if not self.s3_bucket:
            assert ValueError('The s3 bucket name must be provided')
        if not self.s3_key:
            assert ValueError('The s3 key must be provided')
    
    def __get_s3_credentials(self):
        """Get the s3 credentials

        Returns:
            dict: The s3 credentials
        """
        self.log.info("Getting s3 credentials")
        aws_hook = AwsHook(self.aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()
        
        return aws_credentials

    def __get_s3_path(self):
        """Get the s3 path by rendering the context (if provided)
        
        Returns:
            str: The s3 path
            ValueError: If the key is not provided
            ValueError: If the placeholder is not provided in the initialization
        """
        self.log.info("Getting the s3 path")
        rendered_key = self.s3_key.format(**self.context)

        if not rendered_key:
            assert ValueError("The key of the file must be configured")
        
        if  "{" in rendered_key or "}" in rendered_key:
            assert ValueError("The placeholder must be provided in the initialization of the operator")

        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info("The path of S3 to be staged: " + s3_path)
        return s3_path

    def __get_redshift_hook(self):
        """Get the redshift hook instance

        Returns:
            PostgresHook: The redshift hook instance
        """
        return PostgresHook(postgres_conn_id=self.redshift_conn_id)
    
    def __clean_redshift_table(self, redshift):
        """Clean the table in redshift
        
        Args:
            redshift (PostgresHook): The redshift hook instance
        """
        self.log.info("Clearing data from destination Redshift table")
        redshift.run(f"DELETE FROM {self.table}")
        self.log.info("Data cleared from destination Redshift table")

    def __get_copy_query(self, s3_credentials, s3_path):
        """Get the query to be executed as copy command
        
        Args:
            s3_credentials (dict): The s3 credentials
            s3_path (str): The s3 path
            
        Returns:
            str: The formatted sql query
        """
        self.log.info("Creating the query to copy the data from S3 to Redshift")
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            s3_credentials.access_key,
            s3_credentials.secret_key,
            self.copy_params,
        )
        return formatted_sql
      
    def __execute_copy_query(self, redshift, formatted_sql):
        """Execute the copy command
        
        Args:
            redshift (PostgresHook): The redshift hook instance
            formatted_sql (str): The formatted sql query
        """
        self.log.info("Copying data from S3 to Redshift")
        redshift.run(formatted_sql)
        self.log.info("Data copied from S3 to Redshift")

    def execute(self, context):
        """Execute the operator to stage data from S3 to Redshift
        """
        self.log.info("Starting the StageToRedshiftOperator")
        
        # Get the s3 credentials
        s3_credentials = self.__get_s3_credentials()
        
        # Get the s3 path
        s3_path = self.__get_s3_path()

        # Get the redshift hook instance
        redshift = self.__get_redshift_hook()

        # Clean the staging table in redshift
        self.__clean_redshift_table(redshift)
        
        # Get the query to be executed as copy command
        formatted_sql = self.__get_copy_query(s3_credentials, s3_path)
        
        # Execute the copy command
        self.__execute_copy_query(redshift, formatted_sql)





