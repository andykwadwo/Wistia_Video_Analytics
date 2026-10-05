import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

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

# Script generated for node Amazon S3
additionalOptions={}
AmazonS3_node1791079198725_df = spark.read.format("delta").options(**additionalOptions).load("s3://wistia-analytics-silver-data/visitor/")
AmazonS3_node1791079198725 = DynamicFrame.fromDF(AmazonS3_node1791079198725_df, glueContext, "AmazonS3_node1791079198725")

# Script generated for node Amazon S3
additionalOptions={}
AmazonS3_node1791079178076_df = spark.read.format("delta").options(**additionalOptions).load("s3://wistia-analytics-silver-data/events/")
AmazonS3_node1791079178076 = DynamicFrame.fromDF(AmazonS3_node1791079178076_df, glueContext, "AmazonS3_node1791079178076")

# Script generated for node Join
Join_node1791079252218 = Join.apply(frame1=AmazonS3_node1791079198725, frame2=AmazonS3_node1791079178076, keys1=["visitor_key"], keys2=["visitor_key"], transformation_ctx="Join_node1791079252218")

# Script generated for node Drop Fields
DropFields_node1791079380383 = DropFields.apply(frame=Join_node1791079252218, paths=["`.run_date`", "iframe_heatmap_url", "percent_viewed", "date", "ip", "city", "org", "`.visitor_key`", "last_active_at", "last_event_key", "run_date", "region", "load_count", "play_count", "conversion_type", "media_name", "embed_url", "country"], transformation_ctx="DropFields_node1791079380383")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1791079380383, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1791162409158", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (DropFields_node1791079380383.count() >= 1):
   DropFields_node1791079380383 = DropFields_node1791079380383.coalesce(1)
AmazonS3_node1791164283714 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1791079380383, connection_type="s3", format="csv", connection_options={"path": "s3://wistia-analytics-gold-data/dim-media/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1791164283714")

job.commit()