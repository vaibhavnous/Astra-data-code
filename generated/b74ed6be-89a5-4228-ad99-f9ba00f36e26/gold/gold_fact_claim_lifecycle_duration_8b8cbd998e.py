"""AUTO-GENERATED MICROSOFT FABRIC GOLD SCRIPT"""
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, concat_ws, count, current_timestamp, date_trunc, lit, max, min, sha2, sum

spark = SparkSession.builder.getOrCreate()

SOURCE_TABLES = ['Astra_Data_LH.silver_schema.silver_indemnity_outstanding_estimates']
TARGET_TABLE = 'Astra_Data_LH.gold_schema.fact_claim_lifecycle_duration'
GRAIN_COLUMNS = [{'table': 'Astra_Data_LH.silver_schema.silver_indemnity_outstanding_estimates', 'logical_table': 'indemnity_outstanding_estimates', 'column': 'InsertedDate', 'output': 'InsertedDate', 'semantic_type': 'DATE'}]
DIMENSION_CONTRACT = []
MEASURE_COLUMNS = [{'table': 'Astra_Data_LH.silver_schema.silver_indemnity_outstanding_estimates', 'column': 'GrossEstimate', 'aggregation': 'SUM', 'output': 'claim_lifecycle_duration_value'}]
FILTERS = []
JOIN_PATHS = []
KPI_NAME = 'Claim Lifecycle Duration'
GENERATED_RUN_ID = 'b74ed6be-89a5-4228-ad99-f9ba00f36e26'

runtime_context = globals().get("ATHENA_RUNTIME_CONTEXT")
if not isinstance(runtime_context, dict):
    runtime_context = {}
RUN_ID = str(runtime_context.get("runtime_run_id") or GENERATED_RUN_ID)
LOGICAL_WORK_ID = str(runtime_context.get("logical_work_id") or "")

spark.sql("CREATE SCHEMA IF NOT EXISTS Astra_Data_LH.gold_schema")

frames = {}
resolved_columns = {}
for table_name in SOURCE_TABLES:
    if not spark.catalog.tableExists(table_name):
        raise ValueError(f"Missing approved Fabric Silver source: {table_name}")
    frame = spark.table(table_name)
    if LOGICAL_WORK_ID:
        if "_logical_work_id" not in frame.columns:
            raise ValueError(f"Fabric Silver source lacks _logical_work_id: {table_name}")
        frame = frame.withColumn("_logical_work_id", col("_logical_work_id").cast("string"))
        frame = frame.filter(col("_logical_work_id") == lit(LOGICAL_WORK_ID).cast("string"))
    by_name = {}
    for available_name in frame.columns:
        identity = available_name.casefold()
        if identity in by_name:
            raise ValueError(f"Duplicate case-insensitive Silver column in {table_name}: {available_name}")
        by_name[identity] = available_name
    frames[table_name] = frame
    resolved_columns[table_name] = by_name

required_columns = []
required_columns.extend((item["table"], item["column"]) for item in GRAIN_COLUMNS)
required_columns.extend((item["table"], item["column"]) for item in MEASURE_COLUMNS if item["column"] != "*")
required_columns.extend((item["table"], item["column"]) for item in FILTERS)
for item in JOIN_PATHS:
    required_columns.extend(((item["left_table"], item["left_column"]), (item["right_table"], item["right_column"])))
missing_columns = [
    f"{table_name}.{column_name}"
    for table_name, column_name in required_columns
    if column_name.casefold() not in resolved_columns[table_name]
]
if missing_columns:
    raise ValueError(f"Approved Fabric Gold columns are missing: {missing_columns}")

for item in FILTERS:
    frame = frames[item["table"]]
    actual = resolved_columns[item["table"]][item["column"].casefold()]
    operator = item["operator"]
    value = item.get("value")
    if operator == "IN":
        frame = frame.filter(col(actual).isin(value))
    elif operator == "IS NULL":
        frame = frame.filter(col(actual).isNull())
    elif operator == "IS NOT NULL":
        frame = frame.filter(col(actual).isNotNull())
    else:
        predicates = {"=": col(actual) == value, "!=": col(actual) != value, ">": col(actual) > value, ">=": col(actual) >= value, "<": col(actual) < value, "<=": col(actual) <= value}
        frame = frame.filter(predicates[operator])
    frames[item["table"]] = frame

for spec in DIMENSION_CONTRACT:
    source_table = spec["source_table"]
    target_table = spec["target_table"]
    key_column = spec["key_column"]
    natural_columns = list(spec.get("columns") or [])
    if source_table not in frames:
        raise ValueError(f"Fabric Gold dimension source is not loaded: {source_table}")
    source = frames[source_table]
    missing_natural = [
        name
        for name in natural_columns
        if name.casefold() not in resolved_columns[source_table]
    ]
    if missing_natural:
        raise ValueError(f"Fabric Gold dimension columns are missing: {target_table} {missing_natural}")
    dim_selects = [
        col(resolved_columns[source_table][name.casefold()]).cast("string").alias(name)
        for name in natural_columns
    ]
    dimension = source.select(*dim_selects).dropDuplicates(natural_columns)
    if natural_columns:
        dimension = dimension.fillna({name: "UNKNOWN" for name in natural_columns})
    natural_key_inputs = [col(name).cast("string") for name in natural_columns] or [lit("__ALL__").cast("string")]
    dimension = (
        dimension
        .withColumn(key_column, sha2(concat_ws("||", *natural_key_inputs), 256))
        .withColumn("gold_run_id", lit(RUN_ID))
        .withColumn("gold_processed_timestamp", current_timestamp())
        .withColumn("_logical_work_id", lit(LOGICAL_WORK_ID).cast("string"))
    )
    if dimension.groupBy(key_column).count().filter(col("count") > 1).limit(1).count():
        raise ValueError(f"Fabric Gold dimension keys are not unique for {target_table}")
    dim_view = "fabric_gold_dim_" + spec["entity"]
    dimension.createOrReplaceTempView(dim_view)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target_table} USING DELTA AS SELECT * FROM {dim_view} WHERE 1 = 0")
    dim_target_columns = spark.table(target_table).columns
    dim_missing_columns = sorted(set(dimension.columns) - set(dim_target_columns))
    if dim_missing_columns:
        raise ValueError(f"Fabric Gold dimension target requires reviewed schema evolution: {dim_missing_columns}")
    dim_common_columns = [name for name in dim_target_columns if name in dimension.columns]
    dim_update_columns = [name for name in dim_common_columns if name != key_column]
    dim_update_assignments = {name: f"source.`{name.replace('`', '``')}`" for name in dim_update_columns}
    dim_insert_assignments = {name: f"source.`{name.replace('`', '``')}`" for name in dim_common_columns}
    dim_merge_condition = f"target.`{key_column}` = source.`{key_column}`"
    dim_merge = DeltaTable.forName(spark, target_table).alias("target").merge(dimension.alias("source"), dim_merge_condition)
    if dim_update_assignments:
        dim_merge = dim_merge.whenMatchedUpdate(set=dim_update_assignments)
    dim_merge.whenNotMatchedInsert(values=dim_insert_assignments).execute()

aliases = {table_name: f"s{index}" for index, table_name in enumerate(SOURCE_TABLES)}
joined_tables = {SOURCE_TABLES[0]}
working = frames[SOURCE_TABLES[0]].alias(aliases[SOURCE_TABLES[0]])
for item in JOIN_PATHS:
    left_table = item["left_table"]
    right_table = item["right_table"]
    if left_table not in joined_tables or right_table in joined_tables:
        raise ValueError("Fabric Gold joins must form an ordered, direct path from the primary source")
    before_count = working.count()
    right = frames[right_table].alias(aliases[right_table])
    left_column = resolved_columns[left_table][item["left_column"].casefold()]
    right_column = resolved_columns[right_table][item["right_column"].casefold()]
    working = working.join(
        right,
        col(f"{aliases[left_table]}.`{left_column}`") == col(f"{aliases[right_table]}.`{right_column}`"),
        item["join_type"],
    )
    after_count = working.count()
    allowed_count = (before_count if before_count > 0 else 1) * float(item["max_row_multiplier"])
    if after_count > allowed_count:
        raise ValueError(f"Fabric Gold join exceeded approved row multiplier for {right_table}")
    joined_tables.add(right_table)

grain_expressions = []
for item in GRAIN_COLUMNS:
    source = col(f"{aliases[item['table']]}.`{resolved_columns[item['table']][item['column'].casefold()]}`")
    grain_expressions.append(
        date_trunc(item["time_grain"], source).alias(item["output"])
        if item.get("time_grain")
        else source.alias(item["output"])
    )
measure_expressions = []
for item in MEASURE_COLUMNS:
    actual = None if item["column"] == "*" else resolved_columns[item["table"]][item["column"].casefold()]
    operations = {"SUM": sum, "COUNT": count, "AVG": avg, "MIN": min, "MAX": max}
    aggregation = item["aggregation"]
    if aggregation == "COUNT":
        measure_expressions.append(count(lit(1)).alias(item["output"]))
    else:
        source = col(f"{aliases[item['table']]}.`{actual}`")
        if aggregation in {"SUM", "AVG"}:
            source = source.cast("double")
        measure_expressions.append(operations[aggregation](source).alias(item["output"]))

if grain_expressions:
    output = working.groupBy(*grain_expressions).agg(*measure_expressions)
else:
    output = working.agg(*measure_expressions)

grain_names = [item["output"] for item in GRAIN_COLUMNS]
for name in grain_names:
    output = output.withColumn(name, col(name).cast("string"))
if grain_names:
    output = output.fillna({name: "UNKNOWN" for name in grain_names})
dimension_key_columns = []
dimension_grain_outputs = set()
for spec in DIMENSION_CONTRACT:
    grain_outputs = [name for name in spec.get("grain_outputs", []) if name in output.columns]
    if not grain_outputs:
        continue
    key_column = spec["key_column"]
    output = output.withColumn(
        key_column,
        sha2(concat_ws("||", *[col(name).cast("string") for name in grain_outputs]), 256),
    )
    dimension_key_columns.append(key_column)
    dimension_grain_outputs.update(grain_outputs)
if dimension_grain_outputs:
    output = output.drop(*sorted(dimension_grain_outputs))
fact_grain_names = [name for name in grain_names if name not in dimension_grain_outputs]
key_inputs = [col(name).cast("string") for name in [*fact_grain_names, *dimension_key_columns]] or [lit(KPI_NAME).cast("string")]
output = output.withColumn("gold_upsert_key", sha2(concat_ws("||", *key_inputs), 256))
if output.groupBy("gold_upsert_key").count().filter(col("count") > 1).limit(1).count():
    raise ValueError(f"Gold upsert keys are not unique for {TARGET_TABLE}")
output = (
    output
    .withColumn("gold_run_id", lit(RUN_ID))
    .withColumn("gold_processed_timestamp", current_timestamp())
    .withColumn("kpi_name", lit(KPI_NAME))
    .withColumn("_logical_work_id", lit(LOGICAL_WORK_ID).cast("string"))
)

TEMP_VIEW = "fabric_gold_source_" + 'fact_claim_lifecycle_duration'
output.createOrReplaceTempView(TEMP_VIEW)
spark.sql(f"CREATE TABLE IF NOT EXISTS {TARGET_TABLE} USING DELTA AS SELECT * FROM {TEMP_VIEW} WHERE 1 = 0")
target_columns = spark.table(TARGET_TABLE).columns
missing_target_columns = sorted(set(output.columns) - set(target_columns))
if missing_target_columns:
    raise ValueError(f"Fabric Gold target requires reviewed schema evolution: {missing_target_columns}")
common_columns = [name for name in target_columns if name in output.columns]
update_columns = [name for name in common_columns if name != "gold_upsert_key"]

def _source_assignment(column_name):
    return f"source.`{column_name.replace('`', '``')}`"

update_assignments = {name: _source_assignment(name) for name in update_columns}
insert_assignments = {name: _source_assignment(name) for name in common_columns}
merge_condition = "target.`gold_upsert_key` = source.`gold_upsert_key`"
merge_builder = DeltaTable.forName(spark, TARGET_TABLE).alias("target").merge(output.alias("source"), merge_condition)
if update_assignments:
    merge_builder = merge_builder.whenMatchedUpdate(set=update_assignments)
merge_builder.whenNotMatchedInsert(values=insert_assignments).execute()

print(f"SUCCESS: Fabric Gold merge completed for {TARGET_TABLE}")
