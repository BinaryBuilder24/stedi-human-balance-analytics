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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1787252317946 = glueContext.create_dynamic_frame.from_catalog(database="customer_db", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1787252317946")

# Script generated for node Customers Curated
CustomersCurated_node1787252754066 = glueContext.create_dynamic_frame.from_catalog(database="customer_db", table_name="customers_curated", transformation_ctx="CustomersCurated_node1787252754066")

# Script generated for node Filter Step Trainer
SqlQuery2766 = '''
SELECT s.*
FROM s
WHERE s.serialnumber IN (
    SELECT c.serialnumber
    FROM c
)
'''
FilterStepTrainer_node1787252828712 = sparkSqlQuery(glueContext, query = SqlQuery2766, mapping = {"c":CustomersCurated_node1787252754066, "s":StepTrainerLanding_node1787252317946}, transformation_ctx = "FilterStepTrainer_node1787252828712")

# Script generated for node Step Trainer Trusted
EvaluateDataQuality().process_rows(frame=FilterStepTrainer_node1787252828712, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1787252250063", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrusted_node1787252928228 = glueContext.getSink(path="s3://stedi-data-lake-guljahan/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1787252928228")
StepTrainerTrusted_node1787252928228.setCatalogInfo(catalogDatabase="customer_db",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1787252928228.setFormat("json")
StepTrainerTrusted_node1787252928228.writeFrame(FilterStepTrainer_node1787252828712)
job.commit()