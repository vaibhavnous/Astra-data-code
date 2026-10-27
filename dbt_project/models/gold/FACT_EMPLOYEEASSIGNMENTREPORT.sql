-- Astra Gold changes against referenced branch:
-- ADDITION FACT_EMPLOYEEASSIGNMENTREPORT: The current Gold design produces a model absent from the referenced branch.
-- Evidence: metadata/astra_gold_column_change_rationale.md
-- Astra incremental fact: merge by declared grain
WITH fact_source AS (
    SELECT
        base_dim."ENO" AS "ENO",
        base_dim."ENAME" AS "ENAME",
        base_dim."EMAIL" AS "EMAIL",
        base_dim."JOIN_DATE" AS "JOINDATE",
        dimension_1."DEPARTMENT_ID" AS "DEPARTMENT_ID",
        dimension_1."LOCATION" AS "LOCATION",
        CAST('ASTRA_DATA' AS VARCHAR) AS "created_by",
        CAST('ASTRA_DATA' AS VARCHAR) AS "modified_by",
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS "created_date",
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS "modified_date"
    FROM {{ ref('DIM_EMPLOYEE') }} AS base_dim
    LEFT JOIN {{ ref('FACT_EMPLOYEE_DEPARTMENT') }} AS relationship_1 ON base_dim."HUB_HK" = relationship_1."EMPLOYEE_HUB_HK"
    LEFT JOIN {{ ref('DIM_DEPARTMENT') }} AS dimension_1 ON relationship_1."DEPARTMENT_HUB_HK" = dimension_1."HUB_HK" AND dimension_1."astra_is_current" = TRUE
    WHERE base_dim."astra_is_current" = TRUE
)
{% if astra_is_incremental() %}
SELECT
    source_row."ENO" AS "ENO",
    source_row."ENAME" AS "ENAME",
    source_row."EMAIL" AS "EMAIL",
    source_row."JOINDATE" AS "JOINDATE",
    source_row."DEPARTMENT_ID" AS "DEPARTMENT_ID",
    source_row."LOCATION" AS "LOCATION",
    COALESCE(target_row."created_by", source_row."created_by") AS "created_by",
    source_row."modified_by" AS "modified_by",
    COALESCE(target_row."created_date", source_row."created_date") AS "created_date",
    source_row."modified_date" AS "modified_date"
FROM fact_source AS source_row
LEFT JOIN {{ this }} AS target_row ON target_row."ENO" = source_row."ENO"
WHERE target_row."ENO" IS NULL
    OR source_row."ENAME" IS DISTINCT FROM target_row."ENAME"
    OR source_row."EMAIL" IS DISTINCT FROM target_row."EMAIL"
    OR source_row."JOINDATE" IS DISTINCT FROM target_row."JOINDATE"
    OR source_row."DEPARTMENT_ID" IS DISTINCT FROM target_row."DEPARTMENT_ID"
    OR source_row."LOCATION" IS DISTINCT FROM target_row."LOCATION"
{% else %}
SELECT
    source_row."ENO" AS "ENO",
    source_row."ENAME" AS "ENAME",
    source_row."EMAIL" AS "EMAIL",
    source_row."JOINDATE" AS "JOINDATE",
    source_row."DEPARTMENT_ID" AS "DEPARTMENT_ID",
    source_row."LOCATION" AS "LOCATION",
    source_row."created_by" AS "created_by",
    source_row."modified_by" AS "modified_by",
    source_row."created_date" AS "created_date",
    source_row."modified_date" AS "modified_date"
FROM fact_source AS source_row
{% endif %}
