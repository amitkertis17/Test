import sys
import boto3
from  pyspark.sql.functions import input_file_name
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
from pyspark.sql import SparkSession
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

session_id = sys.argv[5]
print(f"Glue ETL Session ID: {session_id}")
#set var

dde_bucket = 'as-data-gadi'
dde_prefix = 'adstra-load-test/'

ENV_NAME= 'pioneer'
PARTNER_NAME = 'adstra'
PNRAS_BUCKET = 'pnras-bucket'
RESULT_OUTPUT_LOCATION = f"s3://{PNRAS_BUCKET}/{PARTNER_NAME}/athena/athena-results/{ENV_NAME}"


from datetime import datetime

current_timestamp = datetime.now()
formatted_timestamp = current_timestamp.strftime("%Y%m%d_%H_%M")


# Print the current timestamp
print(formatted_timestamp)

# Find the latest folder in Adstra dir. 
import boto3
s3_client = boto3.client('s3')

response = s3_client.list_objects(Bucket=dde_bucket, Prefix=dde_prefix, Delimiter='/')
folders = [content['Prefix'] for content in response.get('CommonPrefixes', [])]
#print(folders)
latest_folder = sorted(folders)[-1]
print(latest_folder)
# Split the string by the equals sign and get the second part
date_part = latest_folder.split('=')[1]

# Remove the trailing slash
date_part = date_part.rstrip('/')

print(date_part)
# Load daily exposures

adstra_daily_exposures_path = f"s3://{dde_bucket}/{latest_folder}"
#for debug i am taking only one file 
adstra_daily_exposures_path='s3://as-data-gadi/adstra-load-test/partition_date=2023-09-30/part-03359-d0f91847-819a-4e4e-8a6a-bc3491a874fc.c000.snappy.parquet'

adstra_daily_exposures = spark \
    .read \
    .format("parquet") \
    .load(adstra_daily_exposures_path)

adstra_daily_exposures.show(5, False)
adstra_daily_exposures = adstra_daily_exposures.withColumn("input_file", input_file_name())

adstra_daily_exposures.show(5, False)
from pyspark.sql.functions import regexp_extract

# Assuming 'date_part' is the name of the new column
adstra_daily_exposures = adstra_daily_exposures.withColumn(
    "partition_date",
    regexp_extract(adstra_daily_exposures["input_file"], r'partition_date=(\d{4}-\d{2}-\d{2})', 1)
)

adstra_daily_exposures.show(5, False)
adstra_daily_exposures.createOrReplaceTempView("adstra_daily_exposures")

# File monitoring
query = f"""
SELECT input_file, count(input_file) as count
FROM adstra_daily_exposures
GROUP BY input_file
ORDER BY count(input_file) ASC
"""
query_df = spark.sql(query)
query_df.show(5, False)
#save result to s3 

output_s3_path = f"{RESULT_OUTPUT_LOCATION}/File_monitoring/File_monitoring_{date_part}.parquet"
print(f"save result to {output_s3_path}")

query_df.write.parquet(output_s3_path, mode="overwrite")
# Load Data validation Rules from s3 text file

# Copy rules from git:
# aws s3 cp ./partners/Adstra/ruleset.txt s3://as-data-gadi/adstra-info/

import boto3

# Initialize a boto3 S3 client
s3_client = boto3.client('s3')

# Specify the bucket name and the file/key you want to read
file_key = 'adstra-info/ruleset.txt'
ruleset = None

# Use the S3 client to read the contents of the file
try:
    response = s3_client.get_object(Bucket=dde_bucket, Key=file_key)
    ruleset = response['Body'].read().decode('utf-8')
    # file_contents will now contain the contents of the text file as a string
    print(ruleset)
except Exception as e:
    print(f"Error reading the file: {e}")
from awsglue.dynamicframe import DynamicFrame
from awsgluedq.transforms import EvaluateDataQuality

adstra_daily_exposures_dynamic_frame = DynamicFrame.fromDF(adstra_daily_exposures, glueContext, "adstra_daily_exposures_dynamic_frame")

EvaluateDataQualityMultiframe = EvaluateDataQuality().process_rows(
    frame=adstra_daily_exposures_dynamic_frame,
    ruleset=ruleset,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQualityMultiframe",
        "enableDataQualityCloudWatchMetrics": False,
        "enableDataQualityResultsPublishing": False,
    },
    additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
) 

ruleOutcomes = SelectFromCollection.apply(
    dfc=EvaluateDataQualityMultiframe,
    key="ruleOutcomes",
    transformation_ctx="ruleOutcomes",
)

# rowLevelOutcomes_df = rowLevelOutcomes.toDF() # Convert Glue DynamicFrame to SparkSQL DataFrame
# rowLevelOutcomes_df.show(truncate=False) 
# rowLevelOutcomes_df_passed = rowLevelOutcomes_df.filter(rowLevelOutcomes_df.DataQualityEvaluationResult == "Passed") # Filter only the Passed records.
# rowLevelOutcomes_df_failed = rowLevelOutcomes_df.filter(rowLevelOutcomes_df.DataQualityEvaluationResult == "Failed")
# rowLevelOutcomes_df_failed.show(5, truncate=False) # Review the Failed records                    
                
ruleOutcomes.toDF().show(5,False)
from pyspark.sql.functions import lit


df_ruleOutcomes = ruleOutcomes.toDF()
#latest_folder="adstra-load-test/partition_date=2023-10-24/"

df_ruleOutcomes = df_ruleOutcomes.withColumn("input_file", lit(latest_folder))
df_ruleOutcomes = df_ruleOutcomes.withColumn("env_name", lit(ENV_NAME))
df_ruleOutcomes = df_ruleOutcomes.withColumn("run_datetime", lit(current_timestamp))


df_ruleOutcomes.show(5, False)
#save result to s3 

output_s3_path = f"{RESULT_OUTPUT_LOCATION}/Rule_Outcomes/ruleOutcomes_{date_part}_{formatted_timestamp}.parquet"
print(f"save result to {output_s3_path}")

df_ruleOutcomes.write.parquet(output_s3_path, mode="overwrite")

# #filterd result 

# filtered_ruleOutcomes_df = df_ruleOutcomes[df_ruleOutcomes['Outcome'] == 'Failed']
# filtered_ruleOutcomes_df.show(5,False)

# #save result to s3 

# output_s3_path = f"{RESULT_OUTPUT_LOCATION}/Filterd_Rule_Outcomes/filterd_ruleOutcomes_{date_part}.parquet"
# print(f"save result to {output_s3_path}")

# filtered_ruleOutcomes_df.write.parquet(output_s3_path, mode="overwrite")
df_ruleOutcomes.createOrReplaceTempView("ruleOutcomes")

query = f"""
SELECT 
 '{latest_folder}' as s3_path,
  SUM(CASE WHEN Outcome='Failed' THEN 1 ELSE 0 END) as failed_rules,
  SUM(CASE WHEN Outcome='Passed' THEN 1 ELSE 0 END) as passed_rules,
  current_timestamp() as run_datetime,
  '{ENV_NAME}' as env_name
FROM ruleOutcomes
"""
summery_df = spark.sql(query)
summery_df.show(10, False)


#save to s3 

output_s3_path = f"{RESULT_OUTPUT_LOCATION}/summary/file_validation_run_{date_part}_{formatted_timestamp}.parquet"
print(f"save result to {output_s3_path}")

summery_df.write.parquet(output_s3_path, mode="overwrite")

# Assuming you have a PySpark DataFrame 'summery_df'

# Collect the value of the 'failed_rules' column from the first row
failed_rules_value = summery_df.collect()[0]['failed_rules']

# Check if 'failed_rules_value' is equal to 0
if failed_rules_value == 0:
    result = "SUCCEEDED"
else:
    result = "FAILED"

# Print the result
print("Result:", result)

#send SNS 

# Create a message string from the query result
message = f"Environment: {summery_df.collect()[0]['env_name']}\n"
message += f"Validation Result: {result}\n"
message += f"---------------\n"
message += f"Run DateTime: {summery_df.collect()[0]['run_datetime']}\n"
message += f"Number of Failed: {summery_df.collect()[0]['failed_rules']}\n"
message += f"Number of Passed: {summery_df.collect()[0]['passed_rules']}\n"
message += f"S3 Path: {summery_df.collect()[0]['s3_path']}"

# Print the message for reference
print(f'Message:\n{message}')

# Initialize the SNS client
sns_client = boto3.client('sns', region_name='us-east-1')

# Define the SNS topic ARN
topic_arn = 'arn:aws:sns:us-east-1:752787015194:pioneer_file_validation'

# Publish the message to the SNS topic
response = sns_client.publish(
    TopicArn=topic_arn,
    Message=message
)

print(f'Message sent with message ID: {response["MessageId"]}')

job.commit()