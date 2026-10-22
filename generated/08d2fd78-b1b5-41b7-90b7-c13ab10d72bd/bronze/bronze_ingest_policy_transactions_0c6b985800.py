"""
AUTO-GENERATED MICROSOFT FABRIC BRONZE SCRIPT

Source: Files/astra_landing/insurance/dbo/policy_transactions/policy_transactions.parquet
Target Lakehouse: Astra_Data_LH
Target Delta Table: Astra_Data_LH.bronze_schema.bronze_policy_transactions
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

spark = SparkSession.builder.getOrCreate()

SOURCE_PATH = 'Files/astra_landing/insurance/dbo/policy_transactions/policy_transactions.parquet'
TARGET_LAKEHOUSE = 'Astra_Data_LH'
TARGET_SCHEMA = 'bronze_schema'
TARGET_TABLE = 'Astra_Data_LH.bronze_schema.bronze_policy_transactions'
MAPPED_COLUMNS = [{'source': 'RERERENCE_ID', 'target': 'RERERENCE_ID', 'type': 'bigint'}, {'source': 'POLICY_ID', 'target': 'POLICY_ID', 'type': 'string'}, {'source': 'BEGIN_DATE', 'target': 'BEGIN_DATE', 'type': 'timestamp'}, {'source': 'END_DATE', 'target': 'END_DATE', 'type': 'timestamp'}, {'source': 'POLICY_ISSUED_DATE', 'target': 'POLICY_ISSUED_DATE', 'type': 'timestamp'}, {'source': 'POLICY_TRANSACTION_ID', 'target': 'POLICY_TRANSACTION_ID', 'type': 'string'}, {'source': 'POLICY_TRANSACTION_TYPE', 'target': 'POLICY_TRANSACTION_TYPE', 'type': 'string'}, {'source': 'BRANCH_OFFICE_ID', 'target': 'BRANCH_OFFICE_ID', 'type': 'string'}, {'source': 'BRANCH_OFFICE_NAME', 'target': 'BRANCH_OFFICE_NAME', 'type': 'int'}, {'source': 'CLIENT_FULL_NAME', 'target': 'CLIENT_FULL_NAME', 'type': 'string'}, {'source': 'PRODUCT_ID', 'target': 'PRODUCT_ID', 'type': 'int'}, {'source': 'PRODUCT_NAME', 'target': 'PRODUCT_NAME', 'type': 'string'}, {'source': 'PRODUCT_GROUP_NAME', 'target': 'PRODUCT_GROUP_NAME', 'type': 'string'}, {'source': 'SEGMENT_NAME', 'target': 'SEGMENT_NAME', 'type': 'string'}, {'source': 'BUSINESS_DIVISION_NAME', 'target': 'BUSINESS_DIVISION_NAME', 'type': 'string'}, {'source': 'AGENT_ID', 'target': 'AGENT_ID', 'type': 'double'}, {'source': 'AGENT_NAME', 'target': 'AGENT_NAME', 'type': 'string'}, {'source': 'AGENT_CATEGORY_ID', 'target': 'AGENT_CATEGORY_ID', 'type': 'string'}, {'source': 'AGEN_T_CATEGORY_NAME', 'target': 'AGEN_T_CATEGORY_NAME', 'type': 'string'}, {'source': 'AGENT_SUB_CATEGORY_NAME', 'target': 'AGENT_SUB_CATEGORY_NAME', 'type': 'string'}, {'source': 'CHANNEL_ID', 'target': 'CHANNEL_ID', 'type': 'int'}, {'source': 'CHANNEL_NAME', 'target': 'CHANNEL_NAME', 'type': 'string'}, {'source': 'CHANNEL_GROUP_ID', 'target': 'CHANNEL_GROUP_ID', 'type': 'int'}, {'source': 'CHANNEL_GROUP_NAME', 'target': 'CHANNEL_GROUP_NAME', 'type': 'string'}, {'source': 'SEGMENT_DIVISION_NAME', 'target': 'SEGMENT_DIVISION_NAME', 'type': 'string'}, {'source': 'MODE_OF_ISSUANCE_ID', 'target': 'MODE_OF_ISSUANCE_ID', 'type': 'string'}, {'source': 'MODE_OF_ISSUANCE_NAME', 'target': 'MODE_OF_ISSUANCE_NAME', 'type': 'string'}, {'source': 'IS_AADHAAR_ATTACHED', 'target': 'IS_AADHAAR_ATTACHED', 'type': 'string'}, {'source': 'IS_PAN_ATTACHED', 'target': 'IS_PAN_ATTACHED', 'type': 'string'}, {'source': 'PREMIUM', 'target': 'PREMIUM', 'type': 'string'}, {'source': 'COMMISSION', 'target': 'COMMISSION', 'type': 'string'}, {'source': 'EXPENSES', 'target': 'EXPENSES', 'type': 'string'}, {'source': 'SERVICE_TAX', 'target': 'SERVICE_TAX', 'type': 'string'}, {'source': 'STAMP_DUTY', 'target': 'STAMP_DUTY', 'type': 'string'}, {'source': 'RISK_SUM_INSURED', 'target': 'RISK_SUM_INSURED', 'type': 'string'}, {'source': 'NO_OF_EMPLOYEES', 'target': 'NO_OF_EMPLOYEES', 'type': 'string'}, {'source': 'NO_O_LIVES', 'target': 'NO_O_LIVES', 'type': 'string'}]
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
    .withColumn("source_table", lit('insurance.dbo.policy_transactions'))
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
