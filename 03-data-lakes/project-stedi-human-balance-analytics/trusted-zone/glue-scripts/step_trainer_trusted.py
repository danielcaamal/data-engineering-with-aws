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

# Script generated for node Customer Curated
CustomerCurated_node1709694603676 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1709694603676",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1709694594637 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing",
    transformation_ctx="StepTrainerLanding_node1709694594637",
)

# Script generated for node Join
Join_node1709694606569 = Join.apply(
    frame1=StepTrainerLanding_node1709694594637,
    frame2=CustomerCurated_node1709694603676,
    keys1=["serialnumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1709694606569",
)

# Script generated for node Drop Fields Manually
SqlQuery0 = """
select distinct serialnumber, sensorreadingtime, distancefromobject 
from myDataSource
"""
DropFieldsManually_node1709696457195 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": Join_node1709694606569},
    transformation_ctx="DropFieldsManually_node1709696457195",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1709694611319 = glueContext.write_dynamic_frame.from_options(
    frame=DropFieldsManually_node1709696457195,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://project-stedi-human-balance-analytics/step_trainer_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node1709694611319",
)

job.commit()
