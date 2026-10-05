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

# Script generated for node Amazon S3
additionalOptions={}
AmazonS3_node1791157349593_df = spark.read.format("delta").options(**additionalOptions).load("s3://wistia-analytics-silver-data/media/")
AmazonS3_node1791157349593 = DynamicFrame.fromDF(AmazonS3_node1791157349593_df, glueContext, "AmazonS3_node1791157349593")

# Script generated for node Amazon S3
additionalOptions={}
AmazonS3_node1791156819903_df = spark.read.format("delta").options(**additionalOptions).load("s3://wistia-analytics-silver-data/events/")
AmazonS3_node1791156819903 = DynamicFrame.fromDF(AmazonS3_node1791156819903_df, glueContext, "AmazonS3_node1791156819903")

# Script generated for node Amazon S3
additionalOptions={}
AmazonS3_node1791156850699_df = spark.read.format("delta").options(**additionalOptions).load("s3://wistia-analytics-silver-data/visitor/")
AmazonS3_node1791156850699 = DynamicFrame.fromDF(AmazonS3_node1791156850699_df, glueContext, "AmazonS3_node1791156850699")

# Script generated for node SQL Query
SqlQuery1295 = '''
select
    e.media_id,
    v.visitor_key as visitor_id,
    e.date,
    m.play_count,
    m.hours_watched as watch_time
from
    event e
join
    visitor v
on
    e.visitor_key = v.visitor_key
join
    media m
on
    e.date = m.date
'''
SQLQuery_node1791157387449 = sparkSqlQuery(glueContext, query = SqlQuery1295, mapping = {"event":AmazonS3_node1791156819903, "visitor":AmazonS3_node1791156850699, "media":AmazonS3_node1791157349593}, transformation_ctx = "SQLQuery_node1791157387449")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1791157387449, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1791156746198", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (SQLQuery_node1791157387449.count() >= 1):
   SQLQuery_node1791157387449 = SQLQuery_node1791157387449.coalesce(1)
AmazonS3_node1791158231472 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1791157387449, connection_type="s3", format="csv", connection_options={"path": "s3://wistia-analytics-gold-data/fact-media-engagement/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1791158231472")

job.commit()