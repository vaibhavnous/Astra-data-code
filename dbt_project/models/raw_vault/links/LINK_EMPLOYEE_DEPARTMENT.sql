WITH normalized AS (
    SELECT
            CAST("ENO" AS NUMBER(38,0)) AS "ENO",
            CAST("DEPARTMENT_ID" AS NUMBER(38,0)) AS "DEPARTMENT_ID",
            NULLIF(TRIM(CAST("DEPARTMENT_NAME" AS VARCHAR(134217728))), '') AS "DEPARTMENT_NAME",
            NULLIF(TRIM(CAST("MANAGER_NAME" AS VARCHAR(134217728))), '') AS "MANAGER_NAME",
            NULLIF(TRIM(CAST("LOCATION" AS VARCHAR(134217728))), '') AS "LOCATION",
            TO_TIMESTAMP_NTZ('{{ env_var("DBT_ATHENA_DV_LOAD_TIMESTAMP") }}') AS "LDTS",
            'BRONZE.DEPARTMENT' AS "RSRC"
        FROM {{ source('bronze_bronze', 'DEPARTMENT') }}
),
keyed AS (
    SELECT
        {{ astra_dv_hash(['DEPARTMENT_ID']) }} AS "DEPARTMENT_HUB_HK",
        {{ astra_dv_hash(['ENO']) }} AS "EMPLOYEE_HUB_HK",
        "LDTS",
        "RSRC"
    FROM normalized
    WHERE "DEPARTMENT_ID" IS NOT NULL AND "ENO" IS NOT NULL
), source_rows AS (
    SELECT
        {{ astra_dv_hash(['DEPARTMENT_HUB_HK', 'EMPLOYEE_HUB_HK']) }} AS "LINK_HK",
        "DEPARTMENT_HUB_HK",
        "EMPLOYEE_HUB_HK",
        "LDTS",
        "RSRC"
    FROM keyed
    WHERE "DEPARTMENT_HUB_HK" IS NOT NULL AND "EMPLOYEE_HUB_HK" IS NOT NULL
), deduplicated AS (
    SELECT
        "LINK_HK", "DEPARTMENT_HUB_HK", "EMPLOYEE_HUB_HK", "LDTS", "RSRC"
    FROM source_rows
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY "LINK_HK"
        ORDER BY "LDTS", "RSRC"
    ) = 1
)
SELECT
    "LINK_HK", "DEPARTMENT_HUB_HK", "EMPLOYEE_HUB_HK", "LDTS", "RSRC"
FROM deduplicated AS source_row
{% if astra_is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} AS existing
    WHERE existing."LINK_HK" = source_row."LINK_HK"
)
{% endif %}
