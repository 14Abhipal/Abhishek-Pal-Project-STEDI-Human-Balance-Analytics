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
CustomerTrusted_node1758778232017 = glueContext.create_dynamic_frame.from_catalog(database="stedi2", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1758778232017")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1758778232282 = glueContext.create_dynamic_frame.from_catalog(database="stedi2", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1758778232282")

# Script generated for node SQL Query
SqlQuery3560 = '''
select distinct cut.* from act inner join cut on act.user=cut.email;
'''
SQLQuery_node1758778274177 = sparkSqlQuery(glueContext, query = SqlQuery3560, mapping = {"cut":CustomerTrusted_node1758778232017, "act":AccelerometerTrusted_node1758778232282}, transformation_ctx = "SQLQuery_node1758778274177")

# Script generated for node Customer Curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758778274177, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758778209567", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCurated_node1758778423169 = glueContext.getSink(path="s3://abhi-bucket-2001/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1758778423169")
CustomerCurated_node1758778423169.setCatalogInfo(catalogDatabase="stedi2",catalogTableName="customer_curated")
CustomerCurated_node1758778423169.setFormat("json")
CustomerCurated_node1758778423169.writeFrame(SQLQuery_node1758778274177)
job.commit()