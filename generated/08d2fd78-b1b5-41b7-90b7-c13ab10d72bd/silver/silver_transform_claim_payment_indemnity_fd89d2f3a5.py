"""
AUTO-GENERATED MICROSOFT FABRIC SILVER SCRIPT

Source: Astra_Data_LH.bronze_schema.bronze_claim_payment_indemnity
Target: Astra_Data_LH.silver_schema.silver_claim_payment_indemnity
"""

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr, lit, trim, when

spark = SparkSession.builder.getOrCreate()

SOURCE_TABLE = 'Astra_Data_LH.bronze_schema.bronze_claim_payment_indemnity'
TARGET_TABLE = 'Astra_Data_LH.silver_schema.silver_claim_payment_indemnity'
MERGE_KEYS = ['PaymentID']
COLUMN_MAPPINGS = [{'source': 'PaymentID', 'target': 'PaymentID', 'type': 'bigint'}, {'source': 'UpdateNum', 'target': 'UpdateNum', 'type': 'int'}, {'source': 'PaidDate', 'target': 'PaidDate', 'type': 'date'}, {'source': 'PaidAmount', 'target': 'PaidAmount', 'type': 'decimal(18,6)'}, {'source': 'ServiceTax', 'target': 'ServiceTax', 'type': 'decimal(18,6)'}, {'source': 'ClaimID', 'target': 'ClaimID', 'type': 'bigint'}, {'source': 'PayeeID', 'target': 'PayeeID', 'type': 'bigint'}, {'source': 'PayeeName', 'target': 'PayeeName', 'type': 'string'}, {'source': 'PayeeType', 'target': 'PayeeType', 'type': 'string'}, {'source': 'ServiceProviderID', 'target': 'ServiceProviderID', 'type': 'bigint'}, {'source': 'ServiceProviderName', 'target': 'ServiceProviderName', 'type': 'string'}, {'source': 'ServiceProviderTypeName', 'target': 'ServiceProviderTypeName', 'type': 'string'}, {'source': 'PaymentModeID', 'target': 'PaymentModeID', 'type': 'string'}, {'source': 'PaymentModeName', 'target': 'PaymentModeName', 'type': 'string'}, {'source': 'SurveyType', 'target': 'SurveyType', 'type': 'string'}, {'source': 'GarageID', 'target': 'GarageID', 'type': 'bigint'}, {'source': 'GarageName', 'target': 'GarageName', 'type': 'string'}, {'source': 'GCGarageCity', 'target': 'GCGarageCity', 'type': 'string'}, {'source': 'GarageTypeID', 'target': 'GarageTypeID', 'type': 'bigint'}, {'source': 'GarageTypeName', 'target': 'GarageTypeName', 'type': 'string'}, {'source': 'HospitalID', 'target': 'HospitalID', 'type': 'bigint'}, {'source': 'HospitalName', 'target': 'HospitalName', 'type': 'string'}, {'source': 'HospitalType', 'target': 'HospitalType', 'type': 'string'}, {'source': 'RERERENCE_ID', 'target': 'RERERENCE_ID', 'type': 'bigint'}]
GENERATED_RUN_ID = '08d2fd78-b1b5-41b7-90b7-c13ab10d72bd'

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

TEMP_VIEW = "fabric_silver_source_" + 'silver_claim_payment_indemnity'
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
