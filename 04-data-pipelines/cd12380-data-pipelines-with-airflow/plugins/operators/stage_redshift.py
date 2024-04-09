from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
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
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 copy_params="",
                 context={},
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.context = context
        self.copy_params = copy_params
        self.__validate_params()

    def __validate_params(self):
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
        self.log.info("Getting s3 credentials")
        aws_hook = AwsHook(self.aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()

        if not aws_credentials:
            assert ValueError('AWS Credentials for S3 must be configured')
        
        return aws_credentials

    def __get_s3_path(self):
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
        return PostgresHook(postgres_conn_id=self.redshift_conn_id)
    
    def __clean_redshift_table(self, redshift):
        self.log.info("Clearing data from destination Redshift table")
        redshift.run(f"DELETE FROM {self.table}")
        self.log.info("Data cleared from destination Redshift table")

    def __get_copy_query(self, s3_credentials, s3_path):
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
        self.log.info("Copying data from S3 to Redshift")
        redshift.run(formatted_sql)
        self.log.info("Data copied from S3 to Redshift")

    def execute(self, context):
        self.log.info("Starting the StageToRedshiftOperator")
        
        # Get the s3 credentials
        s3_credentials = self.__get_s3_credentials()
        
        # Get the s3 path
        s3_path = self.__get_s3_path()

        # Get the redshift hook instance
        redshift = self.__get_redshift_hook()

        # Clean the table in redshift
        self.__clean_redshift_table(redshift)
        
        # Get the query to be executed as copy command
        formatted_sql = self.__get_copy_query(s3_credentials, s3_path)
        
        # Execute the copy command
        self.__execute_copy_query(redshift, formatted_sql)





