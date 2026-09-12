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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1789193707969 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1789193707969")

# Script generated for node Customer Trusted
CustomerTrusted_node1789193706162 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1789193706162")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct sensorReadingTIme, step_trainer_landing.serialNumber, distanceFromObject from step_trainer_landing
INNER JOIN customer_trusted ON step_trainer_landing.serialnumber = customer_trusted.serialnumber
'''
SQLQuery_node1789193716742 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_landing":StepTrainerLanding_node1789193707969, "customer_trusted":CustomerTrusted_node1789193706162}, transformation_ctx = "SQLQuery_node1789193716742")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1789193723506 = glueContext.write_dynamic_frame.from_catalog(frame=SQLQuery_node1789193716742, database="stedi", table_name="step_trainer_trusted", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="StepTrainerTrusted_node1789193723506")

job.commit()