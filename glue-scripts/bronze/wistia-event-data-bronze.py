import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1790986874785 = glueContext.create_dynamic_frame.from_catalog(database="wistia-events-db", table_name="wistia_stats_events", transformation_ctx="AWSGlueDataCatalog_node1790986874785")

# Script generated for node Drop Fields
DropFields_node1790988217546 = DropFields.apply(frame=AWSGlueDataCatalog_node1790986874785, paths=["thumbnail.url", "thumbnail.width", "thumbnail.height", "thumbnail.type", "thumbnail", "user_agent_details.browser", "user_agent_details.browser_version", "user_agent_details.platform", "user_agent_details.mobile", "user_agent_details"], transformation_ctx="DropFields_node1790988217546")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1790988217546, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1790988188010", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
additional_options = {"path": "s3://wistia-analytics-bronze-data/events/", "write.parquet.compression-codec": "snappy"}
AmazonS3_node1790988269269_df = DropFields_node1790988217546.toDF()
AmazonS3_node1790988269269_df.write.format("delta").options(**additional_options).mode("append").save()

job.commit()