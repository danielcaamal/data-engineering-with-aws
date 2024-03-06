import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1709697103360 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1709697103360",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1709697104277 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1709697104277",
)

# Script generated for node SQL Query
SqlQuery0 = """
SELECT * FROM accelerometer_trusted acc
INNER JOIN step_trainer_trusted ste ON ste.sensorReadingTime = acc.timestamp
"""
SQLQuery_node1709698872497 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "accelerometer_trusted": AccelerometerTrusted_node1709697103360,
        "step_trainer_trusted": StepTrainerTrusted_node1709697104277,
    },
    transformation_ctx="SQLQuery_node1709698872497",
)

# Script generated for node Amazon S3
AmazonS3_node1709698915113 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1709698872497,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://project-stedi-human-balance-analytics/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1709698915113",
)

job.commit()
