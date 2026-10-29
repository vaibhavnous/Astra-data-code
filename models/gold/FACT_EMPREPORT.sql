-- Astra incremental fact: merge by declared grain
WITH fact_source AS (
    SELECT
        base_dim."ENO" AS "ENO",
        base_dim."ENAME" AS "ENAME",
        base_dim."CITY" AS "CITY",
        base_dim."COUNTRY" AS "COUNTRY",
        dimension_1."DEPARTMENT_NAME" AS "DEPARTMENT_NAME",
        relationship_1."MANAGER_NAME" AS "MANAGER_NAME",
        base_dim."BASIC_SALARY" AS "BASIC_SALARY",
        base_dim."CURRENCY" AS "CURRENCY",
        dimension_2."PROJECT_NAME" AS "PROJECT_NAME",
        relationship_2."PROJECT_ROLE" AS "PROJECT_ROLE",
        CAST('ASTRA_DATA' AS VARCHAR) AS "created_by",
        CAST('ASTRA_DATA' AS VARCHAR) AS "modified_by",
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS "created_date",
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS "modified_date"
    FROM {{ ref('DIM_EMPLOYEE') }} AS base_dim
    LEFT JOIN {{ ref('FACT_EMPLOYEE_DEPARTMENT') }} AS relationship_1 ON base_dim."HUB_HK" = relationship_1."EMPLOYEE_HUB_HK"
    LEFT JOIN {{ ref('FACT_EMPLOYEE_PROJECT') }} AS relationship_2 ON base_dim."HUB_HK" = relationship_2."EMPLOYEE_HUB_HK"
    LEFT JOIN {{ ref('DIM_DEPARTMENT') }} AS dimension_1 ON relationship_1."DEPARTMENT_HUB_HK" = dimension_1."HUB_HK" AND dimension_1."astra_is_current" = TRUE
    LEFT JOIN {{ ref('DIM_PROJECT') }} AS dimension_2 ON relationship_2."PROJECT_HUB_HK" = dimension_2."HUB_HK" AND dimension_2."astra_is_current" = TRUE
    WHERE base_dim."astra_is_current" = TRUE
)
{% if astra_is_incremental() %}
SELECT
    source_row."ENO" AS "ENO",
    source_row."ENAME" AS "ENAME",
    source_row."CITY" AS "CITY",
    source_row."COUNTRY" AS "COUNTRY",
    source_row."DEPARTMENT_NAME" AS "DEPARTMENT_NAME",
    source_row."MANAGER_NAME" AS "MANAGER_NAME",
    source_row."BASIC_SALARY" AS "BASIC_SALARY",
    source_row."CURRENCY" AS "CURRENCY",
    source_row."PROJECT_NAME" AS "PROJECT_NAME",
    source_row."PROJECT_ROLE" AS "PROJECT_ROLE",
    COALESCE(target_row."created_by", source_row."created_by") AS "created_by",
    source_row."modified_by" AS "modified_by",
    COALESCE(target_row."created_date", source_row."created_date") AS "created_date",
    source_row."modified_date" AS "modified_date"
FROM fact_source AS source_row
LEFT JOIN {{ this }} AS target_row ON target_row."ENO" = source_row."ENO"
WHERE target_row."ENO" IS NULL
    OR source_row."ENAME" IS DISTINCT FROM target_row."ENAME"
    OR source_row."CITY" IS DISTINCT FROM target_row."CITY"
    OR source_row."COUNTRY" IS DISTINCT FROM target_row."COUNTRY"
    OR source_row."DEPARTMENT_NAME" IS DISTINCT FROM target_row."DEPARTMENT_NAME"
    OR source_row."MANAGER_NAME" IS DISTINCT FROM target_row."MANAGER_NAME"
    OR source_row."BASIC_SALARY" IS DISTINCT FROM target_row."BASIC_SALARY"
    OR source_row."CURRENCY" IS DISTINCT FROM target_row."CURRENCY"
    OR source_row."PROJECT_NAME" IS DISTINCT FROM target_row."PROJECT_NAME"
    OR source_row."PROJECT_ROLE" IS DISTINCT FROM target_row."PROJECT_ROLE"
{% else %}
SELECT
    source_row."ENO" AS "ENO",
    source_row."ENAME" AS "ENAME",
    source_row."CITY" AS "CITY",
    source_row."COUNTRY" AS "COUNTRY",
    source_row."DEPARTMENT_NAME" AS "DEPARTMENT_NAME",
    source_row."MANAGER_NAME" AS "MANAGER_NAME",
    source_row."BASIC_SALARY" AS "BASIC_SALARY",
    source_row."CURRENCY" AS "CURRENCY",
    source_row."PROJECT_NAME" AS "PROJECT_NAME",
    source_row."PROJECT_ROLE" AS "PROJECT_ROLE",
    source_row."created_by" AS "created_by",
    source_row."modified_by" AS "modified_by",
    source_row."created_date" AS "created_date",
    source_row."modified_date" AS "modified_date"
FROM fact_source AS source_row
{% endif %}
