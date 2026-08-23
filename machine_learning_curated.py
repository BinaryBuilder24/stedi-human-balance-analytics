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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1787253690334 = glueContext.create_dynamic_frame.from_catalog(database="customer_db", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1787253690334")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1787253627370 = glueContext.create_dynamic_frame.from_catalog(database="customer_db", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1787253627370")

# Script generated for node Join ML Data
SqlQuery2357 = '''
SELECT *
FROM a
INNER JOIN s
ON a.timestamp = s.sensorreadingtime
'''
JoinMLData_node1787253754316 = sparkSqlQuery(glueContext, query = SqlQuery2357, mapping = {"s":StepTrainerTrusted_node1787253690334, "a":AccelerometerTrusted_node1787253627370}, transformation_ctx = "JoinMLData_node1787253754316")

# Script generated for node Machine Learning Created
EvaluateDataQuality().process_rows(frame=JoinMLData_node1787253754316, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1787253553183", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCreated_node1787253833743 = glueContext.getSink(path="s3://stedi-data-lake-guljahan/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCreated_node1787253833743")
MachineLearningCreated_node1787253833743.setCatalogInfo(catalogDatabase="customer_db",catalogTableName="machine_learning_curated")
MachineLearningCreated_node1787253833743.setFormat("json")
MachineLearningCreated_node1787253833743.writeFrame(JoinMLData_node1787253754316)
job.commit()