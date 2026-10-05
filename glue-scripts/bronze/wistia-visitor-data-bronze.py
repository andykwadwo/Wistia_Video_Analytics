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
AWSGlueDataCatalog_node1791047705651 = glueContext.create_dynamic_frame.from_catalog(database="wistia-visitor-db", table_name="wistia_stats_visitor", transformation_ctx="AWSGlueDataCatalog_node1791047705651")

# Script generated for node Drop Fields
DropFields_node1791048337158 = DropFields.apply(frame=AWSGlueDataCatalog_node1791047705651, paths=["visitor_identity.name", "visitor_identity.email", "visitor_identity.org.name", "visitor_identity.org.title", "visitor_identity.org", "visitor_identity", "user_agent_details.browser", "user_agent_details.browser_version", "user_agent_details.platform", "user_agent_details.mobile", "user_agent_details"], transformation_ctx="DropFields_node1791048337158")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1791048337158, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1791047233402", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
additional_options = {"path": "s3://wistia-analytics-bronze-data/visitor/", "write.parquet.compression-codec": "snappy"}
AmazonS3_node1791047823762_df = DropFields_node1791048337158.toDF()
AmazonS3_node1791047823762_df.write.format("delta").options(**additional_options).mode("append").save()

job.commit()