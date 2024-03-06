import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1709692572683 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1709692572683",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1709692588508 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1709692588508",
)

# Script generated for node Join
Join_node1709692580416 = Join.apply(
    frame1=CustomerTrusted_node1709692572683,
    frame2=AccelerometerLanding_node1709692588508,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1709692580416",
)

# Script generated for node Drop Fields
DropFields_node1709692704559 = DropFields.apply(
    frame=Join_node1709692580416,
    paths=[
        "sharewithfriendsasofdate",
        "sharewithpublicasofdate",
        "sharewithresearchasofdate",
        "lastupdatedate",
        "registrationdate",
        "serialnumber",
        "birthday",
        "phone",
        "email",
        "customername",
    ],
    transformation_ctx="DropFields_node1709692704559",
)

# Script generated for node Amazon S3
AmazonS3_node1709692602104 = glueContext.getSink(
    path="s3://project-stedi-human-balance-analytics/accelerometer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1709692602104",
)
AmazonS3_node1709692602104.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
AmazonS3_node1709692602104.setFormat("json")
AmazonS3_node1709692602104.writeFrame(DropFields_node1709692704559)
job.commit()
