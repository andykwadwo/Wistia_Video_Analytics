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
AmazonS3_node1791078281604_df = spark.read.format("delta").options(**additionalOptions).load("s3://wistia-analytics-silver-data/events/")
AmazonS3_node1791078281604 = DynamicFrame.fromDF(AmazonS3_node1791078281604_df, glueContext, "AmazonS3_node1791078281604")

# Script generated for node Drop Fields
DropFields_node1791078414179 = DropFields.apply(frame=AmazonS3_node1791078281604, paths=["region", "city", "percent_viewed", "embed_url", "conversion_type", "iframe_heatmap_url", "media_id", "media_name", "media_url", "run_date", "org", "date"], transformation_ctx="DropFields_node1791078414179")

# Script generated for node Change Schema
ChangeSchema_node1791078525262 = ApplyMapping.apply(frame=DropFields_node1791078414179, mappings=[("ip", "string", "ip_address", "string"), ("country", "string", "country", "string"), ("visitor_key", "string", "visitor_id", "string")], transformation_ctx="ChangeSchema_node1791078525262")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1791078525262, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1791075885029", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (ChangeSchema_node1791078525262.count() >= 1):
   ChangeSchema_node1791078525262 = ChangeSchema_node1791078525262.coalesce(1)
AmazonS3_node1791078633734 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1791078525262, connection_type="s3", format="csv", connection_options={"path": "s3://wistia-analytics-gold-data/dim-visitor/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1791078633734")

job.commit()