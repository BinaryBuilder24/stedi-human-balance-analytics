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

# Script generated for node Customer Landing
CustomerLanding_node1787074095356 = glueContext.create_dynamic_frame.from_catalog(database="customer_db", table_name="customer_landing", transformation_ctx="CustomerLanding_node1787074095356")

# Script generated for node Share With Research
SqlQuery2306 = '''
SELECT *
FROM myDataSource
WHERE sharewithresearchasofdate IS NOT NULL
'''
ShareWithResearch_node1787074290854 = sparkSqlQuery(glueContext, query = SqlQuery2306, mapping = {"myDataSource":CustomerLanding_node1787074095356}, transformation_ctx = "ShareWithResearch_node1787074290854")

# Script generated for node Customer Trusted
EvaluateDataQuality().process_rows(frame=ShareWithResearch_node1787074290854, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1787073890207", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerTrusted_node1787074467250 = glueContext.getSink(path="s3://stedi-data-lake-guljahan/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1787074467250")
CustomerTrusted_node1787074467250.setCatalogInfo(catalogDatabase="customer_db",catalogTableName="customer_trusted")
CustomerTrusted_node1787074467250.setFormat("json")
CustomerTrusted_node1787074467250.writeFrame(ShareWithResearch_node1787074290854)
job.commit()