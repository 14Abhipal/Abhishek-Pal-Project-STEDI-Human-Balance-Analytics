import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1758779158594 = glueContext.create_dynamic_frame.from_catalog(database="stedi2", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1758779158594")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1758779158388 = glueContext.create_dynamic_frame.from_catalog(database="stedi2", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1758779158388")

# Script generated for node SQL Query
SqlQuery3480 = '''
select acct.user, stt.*, acct.x, acct.y, acct.z from stt inner join acct on acct.timestamp=stt.sensorreadingtime;
'''
SQLQuery_node1758779196183 = sparkSqlQuery(glueContext, query = SqlQuery3480, mapping = {"acct":AccelerometerTrusted_node1758779158388, "stt":StepTrainerTrusted_node1758779158594}, transformation_ctx = "SQLQuery_node1758779196183")

# Script generated for node Machine Learning Curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758779196183, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758779153707", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCurated_node1758779332381 = glueContext.getSink(path="s3://abhi-bucket-2001/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1758779332381")
MachineLearningCurated_node1758779332381.setCatalogInfo(catalogDatabase="stedi2",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1758779332381.setFormat("json")
MachineLearningCurated_node1758779332381.writeFrame(SQLQuery_node1758779196183)
job.commit()