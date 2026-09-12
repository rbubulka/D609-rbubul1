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

# Script generated for node Customer Trusted
CustomerTrusted_node1789189474050 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1789189474050")

# Script generated for node Accelerometer landing
Accelerometerlanding_node1789189449384 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="Accelerometerlanding_node1789189449384")

# Script generated for node SQL Query
SqlQuery0 = '''
select `user`, `timestamp`,`x`,`y`,`z` from accelerometer_landing 
LEFT JOIN customer_trusted 
ON email = user 
WHERE accelerometer_landing.`timestamp` >= customer_trusted.shareWithResearchAsOfDate
'''
SQLQuery_node1789190066289 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_trusted":CustomerTrusted_node1789189474050, "accelerometer_landing":Accelerometerlanding_node1789189449384}, transformation_ctx = "SQLQuery_node1789190066289")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1789190372421 = glueContext.write_dynamic_frame.from_catalog(frame=SQLQuery_node1789190066289, database="stedi", table_name="accelerometer_trusted", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="AWSGlueDataCatalog_node1789190372421")

job.commit()