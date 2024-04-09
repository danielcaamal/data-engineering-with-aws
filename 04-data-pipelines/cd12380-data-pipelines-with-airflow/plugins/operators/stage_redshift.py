from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
    """

    copy_sql_json = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}'
        ACCEPTINVCHARS AS '^'
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
                 s3_key_json_location="",
                 delimiter=",",
                 sql_create="",
                 ignore_headers=1,
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
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id
        self.s3_key_json_location = s3_key_json_location
        self.context = context
        self.sql_create = sql_create

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

    def __create_redshift_table(self, redshift):
        if not self.sql_create:
            assert ValueError('The sql create is required')
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DROP TABLE IF EXISTS {}".format(self.table))
        self.log.info("Creating the redshift table")
        redshift.run("{}".format(self.sql_create))

    def __get_copy_query(self, s3_credentials, s3_path):
        formatted_sql = ''
        if not self.s3_key_json_location:
            self.log.info("Creating the query to copy the data from S3 to Redshift")
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                s3_credentials.access_key,
                s3_credentials.secret_key,
                self.ignore_headers,
                self.delimiter
            )
        else:
            self.log.info("Creating the query to copy the data (AS JSON) from S3 to Redshift")
            formatted_sql = StageToRedshiftOperator.copy_sql_json.format(
                self.table,
                s3_path,
                s3_credentials.access_key,
                s3_credentials.secret_key,
                f's3://{self.s3_bucket}/{self.s3_key_json_location}'
            )
        return formatted_sql

    def execute(self, context):
        self.log.info("Starting the StageToRedshiftOperator")
        
        # Get the s3 credentials
        s3_credentials = self.__get_s3_credentials()
        
        # Get the s3 path
        s3_path = self.__get_s3_path()

        # Get the redshift hook instance
        redshift = self.__get_redshift_hook()

        # Clean and create the table in redshift
        self.__create_redshift_table(redshift)
        
        # Get the query to be executed as copy command
        formatted_sql = self.__get_copy_query(s3_credentials, s3_path)
            
        self.log.info("Copying data from S3 to Redshift")
        redshift.run(formatted_sql)





