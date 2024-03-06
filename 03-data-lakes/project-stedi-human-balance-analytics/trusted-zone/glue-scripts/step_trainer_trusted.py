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

# Script generated for node Drop Fields Manually
SqlQuery0 = """
SELECT stl.sensorReadingTime, stl.serialNumber, stl.distanceFromObject  FROM customer_curated cc 
INNER JOIN step_trainer_landing stl on stl.serialnumber = cc.serialnumber
"""
DropFieldsManually_node1709696457195 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "step_trainer_landing": StepTrainerLanding_node1709694594637,
        "customer_curated": CustomerCurated_node1709694603676,
    },
    transformation_ctx="DropFieldsManually_node1709696457195",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1709694611319 = glueContext.getSink(
    path="s3://project-stedi-human-balance-analytics/step_trainer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrusted_node1709694611319",
)
StepTrainerTrusted_node1709694611319.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
StepTrainerTrusted_node1709694611319.setFormat("json")
StepTrainerTrusted_node1709694611319.writeFrame(DropFieldsManually_node1709696457195)
job.commit()
