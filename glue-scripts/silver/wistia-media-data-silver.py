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
AmazonS3_node1791076674377_df = spark.read.format("delta").options(**additionalOptions).load("s3://wistia-analytics-bronze-data/media/")
AmazonS3_node1791076674377 = DynamicFrame.fromDF(AmazonS3_node1791076674377_df, glueContext, "AmazonS3_node1791076674377")

# Script generated for node Change Schema
ChangeSchema_node1791076813483 = ApplyMapping.apply(frame=AmazonS3_node1791076674377, mappings=[("date", "string", "date", "string"), ("load_count", "int", "load_count", "int"), ("play_count", "int", "play_count", "int"), ("hours_watched.double", "double", "hours_watched.double", "double"), ("run_date", "string", "run_date", "string")], transformation_ctx="ChangeSchema_node1791076813483")

# Script generated for node Rename Field
RenameField_node1791077111099 = RenameField.apply(frame=ChangeSchema_node1791076813483, old_name="hours_watched.double", new_name="hours_watched", transformation_ctx="RenameField_node1791077111099")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=RenameField_node1791077111099, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1791075885029", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
additional_options = {"path": "s3://wistia-analytics-silver-data/media/", "write.parquet.compression-codec": "snappy"}
AmazonS3_node1791077193342_df = RenameField_node1791077111099.toDF()
AmazonS3_node1791077193342_df.write.format("delta").options(**additional_options).mode("append").save()

job.commit()