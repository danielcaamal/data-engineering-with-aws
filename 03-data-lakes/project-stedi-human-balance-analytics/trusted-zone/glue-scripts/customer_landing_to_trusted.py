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

# Script generated for node Customer Landing
CustomerLanding_node1709688568957 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_landing",
    transformation_ctx="CustomerLanding_node1709688568957",
)

# Script generated for node SQL Query
SqlQuery0 = """
select * from myDataSource
 where shareWithResearchAsOfDate is not null
"""
SQLQuery_node1709691946562 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": CustomerLanding_node1709688568957},
    transformation_ctx="SQLQuery_node1709691946562",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1709690770390 = glueContext.getSink(
    path="s3://project-stedi-human-balance-analytics/customer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node1709690770390",
)
CustomerTrusted_node1709690770390.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_trusted"
)
CustomerTrusted_node1709690770390.setFormat("json")
CustomerTrusted_node1709690770390.writeFrame(SQLQuery_node1709691946562)
job.commit()
