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
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1789191748771 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1789191748771")

# Script generated for node Customer Trusted
CustomerTrusted_node1789191777920 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1789191777920")

# Script generated for node Curate Customers
SqlQuery0 = '''
select distinct serialnumber, registrationdate, lastupdatedate, sharewithresearchasofdate, sharewithpublicasofdate, sharewithfriendsasofdate 
from customer_trusted INNER JOIN accelerometer_trusted ON email = user

'''
CurateCustomers_node1789191820833 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"accelerometer_trusted":AccelerometerTrusted_node1789191748771, "customer_trusted":CustomerTrusted_node1789191777920}, transformation_ctx = "CurateCustomers_node1789191820833")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1789192090104 = glueContext.write_dynamic_frame.from_catalog(frame=CurateCustomers_node1789191820833, database="stedi", table_name="customer_curated", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="AWSGlueDataCatalog_node1789192090104")

job.commit()