import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

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
CustomerTrusted_node1787239933280 = glueContext.create_dynamic_frame.from_catalog(database="customer_db", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1787239933280")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1787239981097 = glueContext.create_dynamic_frame.from_catalog(database="customer_db", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1787239981097")

# Script generated for node Inner Join
InnerJoin_node1787245561029 = Join.apply(frame1=AccelerometerTrusted_node1787239981097, frame2=CustomerTrusted_node1787239933280, keys1=["user"], keys2=["email"], transformation_ctx="InnerJoin_node1787245561029")

# Script generated for node Change Schema
ChangeSchema_node1787245773426 = ApplyMapping.apply(frame=InnerJoin_node1787245561029, mappings=[("customername", "string", "customername", "string"), ("email", "string", "email", "string"), ("phone", "string", "phone", "string"), ("birthday", "string", "birthday", "string"), ("serialnumber", "string", "serialnumber", "string"), ("registrationdate", "long", "registrationdate", "long"), ("lastupdatedate", "long", "lastupdatedate", "long"), ("sharewithresearchasofdate", "long", "sharewithresearchasofdate", "long"), ("sharewithpublicasofdate", "long", "sharewithpublicasofdate", "long"), ("sharewithfriendsasofdate", "long", "sharewithfriendsasofdate", "long")], transformation_ctx="ChangeSchema_node1787245773426")

# Script generated for node Drop Duplicates
DropDuplicates_node1787250043305 =  DynamicFrame.fromDF(ChangeSchema_node1787245773426.toDF().dropDuplicates(["email"]), glueContext, "DropDuplicates_node1787250043305")

# Script generated for node Customers Curated
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1787250043305, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1787246544746", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomersCurated_node1787248718485 = glueContext.getSink(path="s3://stedi-data-lake-guljahan/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomersCurated_node1787248718485")
CustomersCurated_node1787248718485.setCatalogInfo(catalogDatabase="customer_db",catalogTableName="customers_curated")
CustomersCurated_node1787248718485.setFormat("json")
CustomersCurated_node1787248718485.writeFrame(DropDuplicates_node1787250043305)
job.commit()