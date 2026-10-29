-- Astra incremental fact: merge by declared grain
WITH fact_source AS (
    WITH
    anchor AS (SELECT * FROM {{ ref('LINK_EMPLOYEE_DEPARTMENT') }}),
    sat_1 AS (
        SELECT * FROM {{ ref('SAT_LINK_EMPLOYEE_DEPARTMENT') }}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY "LINK_HK" ORDER BY "LDTS" DESC, "HASHDIFF" DESC
        ) = 1
    )
    SELECT
        anchor."LINK_HK" AS "LINK_HK",
        anchor."DEPARTMENT_HUB_HK" AS "DEPARTMENT_HUB_HK",
        anchor."EMPLOYEE_HUB_HK" AS "EMPLOYEE_HUB_HK",
        anchor."LDTS" AS "LDTS",
        anchor."RSRC" AS "RSRC",
        sat_1."MANAGER_NAME" AS "MANAGER_NAME",
        CAST('ASTRA_DATA' AS VARCHAR) AS "created_by",
        CAST('ASTRA_DATA' AS VARCHAR) AS "modified_by",
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS "created_date",
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS "modified_date"
    FROM anchor
    LEFT JOIN sat_1 ON anchor."LINK_HK" = sat_1."LINK_HK"
)
{% if astra_is_incremental() %}
SELECT
    source_row."LINK_HK" AS "LINK_HK",
    source_row."DEPARTMENT_HUB_HK" AS "DEPARTMENT_HUB_HK",
    source_row."EMPLOYEE_HUB_HK" AS "EMPLOYEE_HUB_HK",
    source_row."LDTS" AS "LDTS",
    source_row."RSRC" AS "RSRC",
    source_row."MANAGER_NAME" AS "MANAGER_NAME",
    COALESCE(target_row."created_by", source_row."created_by") AS "created_by",
    source_row."modified_by" AS "modified_by",
    COALESCE(target_row."created_date", source_row."created_date") AS "created_date",
    source_row."modified_date" AS "modified_date"
FROM fact_source AS source_row
LEFT JOIN {{ this }} AS target_row ON target_row."LINK_HK" = source_row."LINK_HK"
WHERE target_row."LINK_HK" IS NULL
    OR source_row."DEPARTMENT_HUB_HK" IS DISTINCT FROM target_row."DEPARTMENT_HUB_HK"
    OR source_row."EMPLOYEE_HUB_HK" IS DISTINCT FROM target_row."EMPLOYEE_HUB_HK"
    OR source_row."LDTS" IS DISTINCT FROM target_row."LDTS"
    OR source_row."RSRC" IS DISTINCT FROM target_row."RSRC"
    OR source_row."MANAGER_NAME" IS DISTINCT FROM target_row."MANAGER_NAME"
{% else %}
SELECT
    source_row."LINK_HK" AS "LINK_HK",
    source_row."DEPARTMENT_HUB_HK" AS "DEPARTMENT_HUB_HK",
    source_row."EMPLOYEE_HUB_HK" AS "EMPLOYEE_HUB_HK",
    source_row."LDTS" AS "LDTS",
    source_row."RSRC" AS "RSRC",
    source_row."MANAGER_NAME" AS "MANAGER_NAME",
    source_row."created_by" AS "created_by",
    source_row."modified_by" AS "modified_by",
    source_row."created_date" AS "created_date",
    source_row."modified_date" AS "modified_date"
FROM fact_source AS source_row
{% endif %}
