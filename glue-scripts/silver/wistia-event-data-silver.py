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
AmazonS3_node1791075993910_df = spark.read.format("delta").options(**additionalOptions).load("s3://wistia-analytics-bronze-data/events/")
AmazonS3_node1791075993910 = DynamicFrame.fromDF(AmazonS3_node1791075993910_df, glueContext, "AmazonS3_node1791075993910")

# Script generated for node Change Schema
ChangeSchema_node1791076124577 = ApplyMapping.apply(frame=AmazonS3_node1791075993910, mappings=[("received_at", "string", "date", "date"), ("ip", "string", "ip", "string"), ("country", "string", "country", "string"), ("region", "string", "region", "string"), ("city", "string", "city", "string"), ("org", "string", "org", "string"), ("percent_viewed", "double", "percent_viewed", "double"), ("embed_url", "string", "embed_url", "string"), ("conversion_type", "string", "conversion_type", "string"), ("iframe_heatmap_url", "string", "iframe_heatmap_url", "string"), ("visitor_key", "string", "visitor_key", "string"), ("media_id", "string", "media_id", "string"), ("media_name", "string", "media_name", "string"), ("media_url", "string", "media_url", "string"), ("run_date", "string", "run_date", "string")], transformation_ctx="ChangeSchema_node1791076124577")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1791076124577, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1791075885029", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
additional_options = {"path": "s3://wistia-analytics-silver-data/events/", "write.parquet.compression-codec": "snappy"}
AmazonS3_node1791076435117_df = ChangeSchema_node1791076124577.toDF()
AmazonS3_node1791076435117_df.write.format("delta").options(**additional_options).mode("append").save()

job.commit()