"""
AUTO-GENERATED MICROSOFT FABRIC SILVER SCRIPT

Source: Astra_Data_LH.bronze_schema.bronze_policy_transactions
Target: Astra_Data_LH.silver_schema.silver_policy_transactions
"""

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr, lit, trim, when

spark = SparkSession.builder.getOrCreate()

SOURCE_TABLE = 'Astra_Data_LH.bronze_schema.bronze_policy_transactions'
TARGET_TABLE = 'Astra_Data_LH.silver_schema.silver_policy_transactions'
MERGE_KEYS = ['RERERENCE_ID']
COLUMN_MAPPINGS = [{'source': 'RERERENCE_ID', 'target': 'RERERENCE_ID', 'type': 'bigint'}, {'source': 'POLICY_ID', 'target': 'POLICY_ID', 'type': 'string'}, {'source': 'BEGIN_DATE', 'target': 'BEGIN_DATE', 'type': 'timestamp'}, {'source': 'END_DATE', 'target': 'END_DATE', 'type': 'timestamp'}, {'source': 'POLICY_ISSUED_DATE', 'target': 'POLICY_ISSUED_DATE', 'type': 'timestamp'}, {'source': 'POLICY_TRANSACTION_ID', 'target': 'POLICY_TRANSACTION_ID', 'type': 'string'}, {'source': 'POLICY_TRANSACTION_TYPE', 'target': 'POLICY_TRANSACTION_TYPE', 'type': 'string'}, {'source': 'BRANCH_OFFICE_ID', 'target': 'BRANCH_OFFICE_ID', 'type': 'string'}, {'source': 'BRANCH_OFFICE_NAME', 'target': 'BRANCH_OFFICE_NAME', 'type': 'int'}, {'source': 'CLIENT_FULL_NAME', 'target': 'CLIENT_FULL_NAME', 'type': 'string'}, {'source': 'PRODUCT_ID', 'target': 'PRODUCT_ID', 'type': 'int'}, {'source': 'PRODUCT_NAME', 'target': 'PRODUCT_NAME', 'type': 'string'}, {'source': 'PRODUCT_GROUP_NAME', 'target': 'PRODUCT_GROUP_NAME', 'type': 'string'}, {'source': 'SEGMENT_NAME', 'target': 'SEGMENT_NAME', 'type': 'string'}, {'source': 'BUSINESS_DIVISION_NAME', 'target': 'BUSINESS_DIVISION_NAME', 'type': 'string'}, {'source': 'AGENT_ID', 'target': 'AGENT_ID', 'type': 'double'}, {'source': 'AGENT_NAME', 'target': 'AGENT_NAME', 'type': 'string'}, {'source': 'AGENT_CATEGORY_ID', 'target': 'AGENT_CATEGORY_ID', 'type': 'string'}, {'source': 'AGEN_T_CATEGORY_NAME', 'target': 'AGEN_T_CATEGORY_NAME', 'type': 'string'}, {'source': 'AGENT_SUB_CATEGORY_NAME', 'target': 'AGENT_SUB_CATEGORY_NAME', 'type': 'string'}, {'source': 'CHANNEL_ID', 'target': 'CHANNEL_ID', 'type': 'int'}, {'source': 'CHANNEL_NAME', 'target': 'CHANNEL_NAME', 'type': 'string'}, {'source': 'CHANNEL_GROUP_ID', 'target': 'CHANNEL_GROUP_ID', 'type': 'int'}, {'source': 'CHANNEL_GROUP_NAME', 'target': 'CHANNEL_GROUP_NAME', 'type': 'string'}, {'source': 'SEGMENT_DIVISION_NAME', 'target': 'SEGMENT_DIVISION_NAME', 'type': 'string'}, {'source': 'MODE_OF_ISSUANCE_ID', 'target': 'MODE_OF_ISSUANCE_ID', 'type': 'string'}, {'source': 'MODE_OF_ISSUANCE_NAME', 'target': 'MODE_OF_ISSUANCE_NAME', 'type': 'string'}, {'source': 'IS_AADHAAR_ATTACHED', 'target': 'IS_AADHAAR_ATTACHED', 'type': 'string'}, {'source': 'IS_PAN_ATTACHED', 'target': 'IS_PAN_ATTACHED', 'type': 'string'}, {'source': 'PREMIUM', 'target': 'PREMIUM', 'type': 'string'}, {'source': 'COMMISSION', 'target': 'COMMISSION', 'type': 'string'}, {'source': 'EXPENSES', 'target': 'EXPENSES', 'type': 'string'}, {'source': 'SERVICE_TAX', 'target': 'SERVICE_TAX', 'type': 'string'}, {'source': 'STAMP_DUTY', 'target': 'STAMP_DUTY', 'type': 'string'}, {'source': 'RISK_SUM_INSURED', 'target': 'RISK_SUM_INSURED', 'type': 'string'}, {'source': 'NO_OF_EMPLOYEES', 'target': 'NO_OF_EMPLOYEES', 'type': 'string'}, {'source': 'NO_O_LIVES', 'target': 'NO_O_LIVES', 'type': 'string'}]
GENERATED_RUN_ID = '1ba02e90-7434-41e0-a70c-a9aa9d0f4526'

runtime_context = globals().get("ATHENA_RUNTIME_CONTEXT")
if not isinstance(runtime_context, dict):
    runtime_context = {}
RUN_ID = str(runtime_context.get("runtime_run_id") or GENERATED_RUN_ID)
LOGICAL_WORK_ID = str(runtime_context.get("logical_work_id") or "")

spark.sql("CREATE SCHEMA IF NOT EXISTS Astra_Data_LH.silver_schema")

if not spark.catalog.tableExists(SOURCE_TABLE):
    raise ValueError(f"Missing Fabric Bronze source table: {SOURCE_TABLE}")

df = spark.table(SOURCE_TABLE)
if LOGICAL_WORK_ID:
    if "_logical_work_id" not in df.columns:
        raise ValueError(f"Fabric Bronze source lacks _logical_work_id: {SOURCE_TABLE}")
    df = df.withColumn("_logical_work_id", col("_logical_work_id").cast("string"))
    df = df.filter(col("_logical_work_id") == lit(LOGICAL_WORK_ID).cast("string"))
if df.limit(1).count() == 0:
    raise ValueError(f"Fabric Bronze source has no rows for this work item: {SOURCE_TABLE}")

available_by_name = {}
for source_name in df.columns:
    identity = source_name.casefold()
    if identity in available_by_name:
        raise ValueError(f"Duplicate case-insensitive Bronze column: {source_name}")
    available_by_name[identity] = source_name

missing_columns = [
    item["source"]
    for item in COLUMN_MAPPINGS
    if item["source"].casefold() not in available_by_name
]
if missing_columns:
    raise ValueError(f"Approved Fabric Silver columns are missing from {SOURCE_TABLE}: {missing_columns}")

metadata_columns = [
    name
    for name in ["run_id", "ingestion_timestamp", "source_system", "source_table", "_logical_work_id"]
    if name in df.columns
]
select_expressions = [
    col(available_by_name[item["source"].casefold()]).alias(item["target"])
    for item in COLUMN_MAPPINGS
]
selected_targets = {item["target"] for item in COLUMN_MAPPINGS}
select_expressions.extend(col(name) for name in metadata_columns if name not in selected_targets)
df = df.select(*select_expressions)
if "_logical_work_id" in df.columns:
    df = df.withColumn("_logical_work_id", col("_logical_work_id").cast("string"))

for item in COLUMN_MAPPINGS:
    column_name = item["target"]
    if item["type"] == "string":
        df = df.withColumn(
            column_name,
            when(trim(col(column_name)) == "", None).otherwise(trim(col(column_name))),
        )
    escaped_name = column_name.replace("`", "``")
    df = df.withColumn(column_name, expr(f"try_cast(`{escaped_name}` AS {item['type']})"))

missing_merge_keys = [key for key in MERGE_KEYS if key not in df.columns]
if missing_merge_keys:
    raise ValueError(f"Approved Fabric Silver merge keys are missing: {missing_merge_keys}")
if any(df.filter(col(key).isNull()).limit(1).count() for key in MERGE_KEYS):
    raise ValueError(f"Approved Fabric Silver merge keys contain nulls for {TARGET_TABLE}")
if df.groupBy(*MERGE_KEYS).count().filter(col("count") > 1).limit(1).count():
    raise ValueError(f"Approved Fabric Silver merge keys are not unique for {TARGET_TABLE}")

df = (
    df
    .withColumn("silver_run_id", lit(RUN_ID))
    .withColumn("silver_processed_timestamp", current_timestamp())
)

TEMP_VIEW = "fabric_silver_source_" + 'silver_policy_transactions'
df.createOrReplaceTempView(TEMP_VIEW)
spark.sql(
    f"CREATE TABLE IF NOT EXISTS {TARGET_TABLE} USING DELTA "
    f"AS SELECT * FROM {TEMP_VIEW} WHERE 1 = 0"
)

target_columns = spark.table(TARGET_TABLE).columns
source_columns = set(df.columns)
missing_target_columns = sorted(source_columns - set(target_columns))
if missing_target_columns:
    raise ValueError(
        f"Fabric Silver target schema is missing approved columns for {TARGET_TABLE}: "
        f"{missing_target_columns}"
    )

common_columns = [name for name in target_columns if name in source_columns]
update_columns = [name for name in common_columns if name not in MERGE_KEYS]

def _source_assignment(column_name):
    escaped_name = column_name.replace("`", "``")
    return f"source.`{escaped_name}`"

update_assignments = {name: _source_assignment(name) for name in update_columns}
insert_assignments = {name: _source_assignment(name) for name in common_columns}
if not insert_assignments:
    raise ValueError(f"No approved columns are available to merge into {TARGET_TABLE}")

merge_condition = " AND ".join(
    f"target.`{key.replace('`', '``')}` = source.`{key.replace('`', '``')}`"
    for key in MERGE_KEYS
)
delta_target = DeltaTable.forName(spark, TARGET_TABLE)
merge_builder = delta_target.alias("target").merge(df.alias("source"), merge_condition)
if update_assignments:
    merge_builder = merge_builder.whenMatchedUpdate(set=update_assignments)
merge_builder.whenNotMatchedInsert(values=insert_assignments).execute()

print(f"SUCCESS: Fabric Silver merge completed for {TARGET_TABLE}")
