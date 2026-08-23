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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1787159735652 = glueContext.create_dynamic_frame.from_catalog(database="customer_db", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1787159735652")

# Script generated for node Customer Trusted
CustomerTrusted_node1787168706307 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-data-lake-guljahan/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1787168706307")

# Script generated for node Inner Join
InnerJoin_node1787185975362 = Join.apply(frame1=AccelerometerLanding_node1787159735652, frame2=CustomerTrusted_node1787168706307, keys1=["user"], keys2=["email"], transformation_ctx="InnerJoin_node1787185975362")

# Script generated for node Change Schema
ChangeSchema_node1787186148175 = ApplyMapping.apply(frame=InnerJoin_node1787185975362, mappings=[("timestamp", "long", "timestamp", "long"), ("user", "string", "user", "string"), ("x", "double", "x", "double"), ("y", "double", "y", "double"), ("z", "double", "z", "double")], transformation_ctx="ChangeSchema_node1787186148175")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1787186148175, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1787159525435", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1787161127147 = glueContext.getSink(path="s3://stedi-data-lake-guljahan/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1787161127147")
AccelerometerTrusted_node1787161127147.setCatalogInfo(catalogDatabase="customer_db",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1787161127147.setFormat("json")
AccelerometerTrusted_node1787161127147.writeFrame(ChangeSchema_node1787186148175)
job.commit()