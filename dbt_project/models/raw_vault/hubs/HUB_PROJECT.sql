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
source_rows AS (
    SELECT
        {{ astra_dv_hash(['PROJECT_ID']) }} AS "HUB_HK",
        "PROJECT_ID",
        "LDTS",
        "RSRC"
    FROM normalized
    WHERE "PROJECT_ID" IS NOT NULL
), deduplicated AS (
    SELECT
        "HUB_HK",
        "PROJECT_ID",
        "LDTS",
        "RSRC"
    FROM source_rows
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY "HUB_HK"
        ORDER BY "LDTS", "RSRC"
    ) = 1
)
SELECT
    "HUB_HK",
        "PROJECT_ID",
        "LDTS",
        "RSRC"
FROM deduplicated AS source_row
{% if astra_is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} AS existing
    WHERE existing."HUB_HK" = source_row."HUB_HK"
)
{% endif %}
