import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1709693598170 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1709693598170",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1709693599420 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1709693599420",
)

# Script generated for node Join
Join_node1709693608277 = Join.apply(
    frame1=AccelerometerTrusted_node1709693599420,
    frame2=CustomerTrusted_node1709693598170,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1709693608277",
)

# Script generated for node Drop Fields
DropFields_node1709693796399 = DropFields.apply(
    frame=Join_node1709693608277,
    paths=["timestamp", "x", "y", "z", "user"],
    transformation_ctx="DropFields_node1709693796399",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1709693799280 = DynamicFrame.fromDF(
    DropFields_node1709693796399.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1709693799280",
)

# Script generated for node Amazon S3
AmazonS3_node1709693802522 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1709693799280,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://project-stedi-human-balance-analytics/customer_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1709693802522",
)

job.commit()
