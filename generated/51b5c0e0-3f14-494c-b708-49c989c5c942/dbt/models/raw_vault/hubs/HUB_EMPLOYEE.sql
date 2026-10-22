WITH normalized AS (
    SELECT
            CAST("ENO" AS NUMBER(38,0)) AS "ENO",
            NULLIF(TRIM(CAST("ENAME" AS VARCHAR(134217728))), '') AS "ENAME",
            NULLIF(TRIM(CAST("EMAIL" AS VARCHAR(134217728))), '') AS "EMAIL",
            TRY_TO_DATE("JOINDATE") AS "JOIN_DATE",
            TO_TIMESTAMP_NTZ('{{ env_var("DBT_ATHENA_DV_LOAD_TIMESTAMP") }}') AS "LDTS",
            'BRONZE.EMPLOYEE' AS "RSRC"
        FROM {{ source('bronze_bronze', 'EMPLOYEE') }}
),
source_rows AS (
    SELECT
        {{ astra_dv_hash(['ENO']) }} AS "HUB_HK",
        "ENO",
        "LDTS",
        "RSRC"
    FROM normalized
    WHERE "ENO" IS NOT NULL
), deduplicated AS (
    SELECT
        "HUB_HK",
        "ENO",
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
        "ENO",
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
