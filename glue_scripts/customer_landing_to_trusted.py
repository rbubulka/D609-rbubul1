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

# Script generated for node Customer Landing
CustomerLanding_node1789187813607 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="CustomerLanding_node1789187813607")

# Script generated for node SharedWithResearch
SqlQuery0 = '''
select * from customer_landing
WHERE sharewithresearchasofdate IS NOT null AND sharewithresearchasofdate != 0
'''
SharedWithResearch_node1789187912398 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_landing":CustomerLanding_node1789187813607}, transformation_ctx = "SharedWithResearch_node1789187912398")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1789188025440 = glueContext.write_dynamic_frame.from_catalog(frame=SharedWithResearch_node1789187912398, database="stedi", table_name="customer_trusted", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="AWSGlueDataCatalog_node1789188025440")

job.commit()