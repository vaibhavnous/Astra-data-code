WITH normalized AS (
    SELECT
            CAST("ENO" AS NUMBER(38,0)) AS "ENO",
            CAST("PROJECT_ID" AS NUMBER(38,0)) AS "PROJECT_ID",
            NULLIF(TRIM(CAST("PROJECT_NAME" AS VARCHAR(134217728))), '') AS "PROJECT_NAME",
            NULLIF(TRIM(CAST("PROJECT_ROLE" AS VARCHAR(134217728))), '') AS "PROJECT_ROLE",
            TRY_TO_DATE("START_DATE") AS "START_DATE",
            TO_TIMESTAMP_NTZ('{{ env_var("DBT_ATHENA_DV_LOAD_TIMESTAMP") }}') AS "LDTS",
            'BRONZE.EMPLOYEE_PROJECT' AS "RSRC"
        FROM {{ source('bronze_bronze', 'EMPLOYEE_PROJECT') }}
),
keyed AS (
    SELECT
        {{ astra_dv_hash(['PROJECT_ID']) }} AS "PROJECT_HUB_HK",
        {{ astra_dv_hash(['ENO']) }} AS "EMPLOYEE_HUB_HK",
        "LDTS",
        "RSRC"
    FROM normalized
    WHERE "PROJECT_ID" IS NOT NULL AND "ENO" IS NOT NULL
), source_rows AS (
    SELECT
        {{ astra_dv_hash(['PROJECT_HUB_HK', 'EMPLOYEE_HUB_HK']) }} AS "LINK_HK",
        "PROJECT_HUB_HK",
        "EMPLOYEE_HUB_HK",
        "LDTS",
        "RSRC"
    FROM keyed
    WHERE "PROJECT_HUB_HK" IS NOT NULL AND "EMPLOYEE_HUB_HK" IS NOT NULL
), deduplicated AS (
    SELECT
        "LINK_HK", "PROJECT_HUB_HK", "EMPLOYEE_HUB_HK", "LDTS", "RSRC"
    FROM source_rows
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY "LINK_HK"
        ORDER BY "LDTS", "RSRC"
    ) = 1
)
SELECT
    "LINK_HK", "PROJECT_HUB_HK", "EMPLOYEE_HUB_HK", "LDTS", "RSRC"
FROM deduplicated AS source_row
{% if astra_is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} AS existing
    WHERE existing."LINK_HK" = source_row."LINK_HK"
)
{% endif %}
