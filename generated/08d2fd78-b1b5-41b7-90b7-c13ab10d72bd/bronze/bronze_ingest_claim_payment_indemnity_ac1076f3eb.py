"""
AUTO-GENERATED MICROSOFT FABRIC BRONZE SCRIPT

Source: Files/astra_landing/insurance/dbo/claim_payment_indemnity/claim_payment_indemnity.parquet
Target Lakehouse: Astra_Data_LH
Target Delta Table: Astra_Data_LH.bronze_schema.bronze_claim_payment_indemnity
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

spark = SparkSession.builder.getOrCreate()

SOURCE_PATH = 'Files/astra_landing/insurance/dbo/claim_payment_indemnity/claim_payment_indemnity.parquet'
TARGET_LAKEHOUSE = 'Astra_Data_LH'
TARGET_SCHEMA = 'bronze_schema'
TARGET_TABLE = 'Astra_Data_LH.bronze_schema.bronze_claim_payment_indemnity'
MAPPED_COLUMNS = [{'source': 'PaymentID', 'target': 'PaymentID', 'type': 'bigint'}, {'source': 'UpdateNum', 'target': 'UpdateNum', 'type': 'int'}, {'source': 'PaidDate', 'target': 'PaidDate', 'type': 'date'}, {'source': 'PaidAmount', 'target': 'PaidAmount', 'type': 'decimal(18,6)'}, {'source': 'ServiceTax', 'target': 'ServiceTax', 'type': 'decimal(18,6)'}, {'source': 'ClaimID', 'target': 'ClaimID', 'type': 'bigint'}, {'source': 'PayeeID', 'target': 'PayeeID', 'type': 'bigint'}, {'source': 'PayeeName', 'target': 'PayeeName', 'type': 'string'}, {'source': 'PayeeType', 'target': 'PayeeType', 'type': 'string'}, {'source': 'ServiceProviderID', 'target': 'ServiceProviderID', 'type': 'bigint'}, {'source': 'ServiceProviderName', 'target': 'ServiceProviderName', 'type': 'string'}, {'source': 'ServiceProviderTypeName', 'target': 'ServiceProviderTypeName', 'type': 'string'}, {'source': 'PaymentModeID', 'target': 'PaymentModeID', 'type': 'string'}, {'source': 'PaymentModeName', 'target': 'PaymentModeName', 'type': 'string'}, {'source': 'SurveyType', 'target': 'SurveyType', 'type': 'string'}, {'source': 'GarageID', 'target': 'GarageID', 'type': 'bigint'}, {'source': 'GarageName', 'target': 'GarageName', 'type': 'string'}, {'source': 'GCGarageCity', 'target': 'GCGarageCity', 'type': 'string'}, {'source': 'GarageTypeID', 'target': 'GarageTypeID', 'type': 'bigint'}, {'source': 'GarageTypeName', 'target': 'GarageTypeName', 'type': 'string'}, {'source': 'HospitalID', 'target': 'HospitalID', 'type': 'bigint'}, {'source': 'HospitalName', 'target': 'HospitalName', 'type': 'string'}, {'source': 'HospitalType', 'target': 'HospitalType', 'type': 'string'}, {'source': 'RERERENCE_ID', 'target': 'RERERENCE_ID', 'type': 'bigint'}]
GENERATED_RUN_ID = '08d2fd78-b1b5-41b7-90b7-c13ab10d72bd'

runtime_context = globals().get("ATHENA_RUNTIME_CONTEXT")
if not isinstance(runtime_context, dict):
    runtime_context = {}
RUN_ID = str(runtime_context.get("runtime_run_id") or GENERATED_RUN_ID)
LOGICAL_WORK_ID = str(runtime_context.get("logical_work_id") or "")

spark.sql("CREATE SCHEMA IF NOT EXISTS Astra_Data_LH.bronze_schema")

df = spark.read.format("parquet").load(SOURCE_PATH)
if not df.schema or not df.schema.fields:
    raise ValueError("Fabric Bronze source returned an empty schema")

if MAPPED_COLUMNS:
    source_by_name = {}
    for source_name in df.columns:
        key = source_name.casefold()
        if key in source_by_name:
            raise ValueError(f"Duplicate case-insensitive source column: {source_name}")
        source_by_name[key] = source_name
    missing = [item["source"] for item in MAPPED_COLUMNS if item["source"].casefold() not in source_by_name]
    if missing:
        raise ValueError(f"Mapped source columns are missing: {', '.join(missing)}")
    df = df.select(*[
        col(source_by_name[item["source"].casefold()]).cast(item["type"]).alias(item["target"])
        for item in MAPPED_COLUMNS
    ])

df = (
    df
    .withColumn("run_id", lit(RUN_ID))
    .withColumn("_logical_work_id", lit(LOGICAL_WORK_ID).cast("string"))
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_system", lit('database'))
    .withColumn("source_table", lit('insurance.dbo.claim_payment_indemnity'))
    .withColumn("_source_path", lit(SOURCE_PATH))
)

(
    df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print(f"SUCCESS: Fabric Bronze ingestion completed for {TARGET_TABLE}")
