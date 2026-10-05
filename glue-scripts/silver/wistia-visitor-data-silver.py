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
AmazonS3_node1791077489676_df = spark.read.format("delta").options(**additionalOptions).load("s3://wistia-analytics-bronze-data/visitor/")
AmazonS3_node1791077489676 = DynamicFrame.fromDF(AmazonS3_node1791077489676_df, glueContext, "AmazonS3_node1791077489676")

# Script generated for node Change Schema
ChangeSchema_node1791077772477 = ApplyMapping.apply(frame=AmazonS3_node1791077489676, mappings=[("visitor_key", "string", "visitor_key", "string"), ("created_at", "string", "created_at", "date"), ("last_active_at", "string", "last_active_at", "date"), ("last_event_key", "string", "last_event_key", "string"), ("load_count", "int", "load_count", "int"), ("play_count", "int", "play_count", "int"), ("run_date", "string", "run_date", "date")], transformation_ctx="ChangeSchema_node1791077772477")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1791077772477, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1791075885029", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
additional_options = {"path": "s3://wistia-analytics-silver-data/visitor/", "write.parquet.compression-codec": "snappy"}
AmazonS3_node1791077862292_df = ChangeSchema_node1791077772477.toDF()
AmazonS3_node1791077862292_df.write.format("delta").options(**additional_options).mode("append").save()

job.commit()