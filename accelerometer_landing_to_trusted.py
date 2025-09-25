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

# Script generated for node Customer Trusted
CustomerTrusted_node1758776759799 = glueContext.create_dynamic_frame.from_catalog(database="stedi2", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1758776759799")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1758776760089 = glueContext.create_dynamic_frame.from_catalog(database="stedi2", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1758776760089")

# Script generated for node SQL Query
SqlQuery3452 = '''
select distinct al.* from al inner join ct on al.user=ct.email;
'''
SQLQuery_node1758776802883 = sparkSqlQuery(glueContext, query = SqlQuery3452, mapping = {"ct":CustomerTrusted_node1758776759799, "al":AccelerometerLanding_node1758776760089}, transformation_ctx = "SQLQuery_node1758776802883")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758776802883, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758776738998", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1758776865320 = glueContext.getSink(path="s3://abhi-bucket-2001/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1758776865320")
AccelerometerTrusted_node1758776865320.setCatalogInfo(catalogDatabase="stedi2",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1758776865320.setFormat("json")
AccelerometerTrusted_node1758776865320.writeFrame(SQLQuery_node1758776802883)
job.commit()