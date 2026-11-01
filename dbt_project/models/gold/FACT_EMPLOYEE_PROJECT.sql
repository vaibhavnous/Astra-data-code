-- Astra incremental fact: merge by declared grain
WITH fact_source AS (
    WITH
    anchor AS (SELECT * FROM {{ ref('LINK_EMPLOYEE_PROJECT') }}),
    sat_1 AS (
        SELECT * FROM {{ ref('SAT_LINK_EMPLOYEE_PROJECT') }}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY "LINK_HK" ORDER BY "LDTS" DESC, "HASHDIFF" DESC
        ) = 1
    )
    SELECT
        anchor."LINK_HK" AS "LINK_HK",
        anchor."PROJECT_HUB_HK" AS "PROJECT_HUB_HK",
        anchor."EMPLOYEE_HUB_HK" AS "EMPLOYEE_HUB_HK",
        anchor."LDTS" AS "LDTS",
        anchor."RSRC" AS "RSRC",
        sat_1."PROJECT_ROLE" AS "PROJECT_ROLE",
        sat_1."START_DATE" AS "START_DATE",
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
    source_row."PROJECT_HUB_HK" AS "PROJECT_HUB_HK",
    source_row."EMPLOYEE_HUB_HK" AS "EMPLOYEE_HUB_HK",
    source_row."LDTS" AS "LDTS",
    source_row."RSRC" AS "RSRC",
    source_row."PROJECT_ROLE" AS "PROJECT_ROLE",
    source_row."START_DATE" AS "START_DATE",
    COALESCE(target_row."created_by", source_row."created_by") AS "created_by",
    source_row."modified_by" AS "modified_by",
    COALESCE(target_row."created_date", source_row."created_date") AS "created_date",
    source_row."modified_date" AS "modified_date"
FROM fact_source AS source_row
LEFT JOIN {{ this }} AS target_row ON target_row."LINK_HK" = source_row."LINK_HK"
WHERE target_row."LINK_HK" IS NULL
    OR source_row."PROJECT_HUB_HK" IS DISTINCT FROM target_row."PROJECT_HUB_HK"
    OR source_row."EMPLOYEE_HUB_HK" IS DISTINCT FROM target_row."EMPLOYEE_HUB_HK"
    OR source_row."LDTS" IS DISTINCT FROM target_row."LDTS"
    OR source_row."RSRC" IS DISTINCT FROM target_row."RSRC"
    OR source_row."PROJECT_ROLE" IS DISTINCT FROM target_row."PROJECT_ROLE"
    OR source_row."START_DATE" IS DISTINCT FROM target_row."START_DATE"
{% else %}
SELECT
    source_row."LINK_HK" AS "LINK_HK",
    source_row."PROJECT_HUB_HK" AS "PROJECT_HUB_HK",
    source_row."EMPLOYEE_HUB_HK" AS "EMPLOYEE_HUB_HK",
    source_row."LDTS" AS "LDTS",
    source_row."RSRC" AS "RSRC",
    source_row."PROJECT_ROLE" AS "PROJECT_ROLE",
    source_row."START_DATE" AS "START_DATE",
    source_row."created_by" AS "created_by",
    source_row."modified_by" AS "modified_by",
    source_row."created_date" AS "created_date",
    source_row."modified_date" AS "modified_date"
FROM fact_source AS source_row
{% endif %}
