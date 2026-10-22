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
        "PROJECT_ROLE", "START_DATE", "LDTS", "RSRC"
    FROM normalized
    WHERE "PROJECT_ID" IS NOT NULL AND "ENO" IS NOT NULL
), hashed AS (
    SELECT
        {{ astra_dv_hash(['PROJECT_HUB_HK', 'EMPLOYEE_HUB_HK']) }} AS "LINK_HK",
        {{ astra_dv_hash(['PROJECT_ROLE', 'START_DATE']) }} AS "HASHDIFF",
        "PROJECT_ROLE", "START_DATE", "LDTS", "RSRC"
    FROM keyed
), deduplicated AS (
    SELECT
        "LINK_HK",
        "HASHDIFF",
        "PROJECT_ROLE",
        "START_DATE",
        "LDTS",
        "RSRC"
    FROM hashed
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY "LINK_HK", "LDTS"
        ORDER BY "HASHDIFF", "RSRC"
    ) = 1
), new_rows AS (
    SELECT
        "LINK_HK",
        "HASHDIFF",
        "PROJECT_ROLE",
        "START_DATE",
        "LDTS",
        "RSRC"
    FROM deduplicated AS source_row
    {% if astra_is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }} AS existing
        WHERE existing."LINK_HK" = source_row."LINK_HK"
          AND existing."LDTS" = source_row."LDTS"
    )
    {% endif %}
), sequenced AS (
    SELECT
        "LINK_HK",
        "HASHDIFF",
        "PROJECT_ROLE",
        "START_DATE",
        "LDTS",
        "RSRC",
        LAG("HASHDIFF") OVER (
            PARTITION BY "LINK_HK"
            ORDER BY "LDTS", "HASHDIFF"
        ) AS "PREVIOUS_HASHDIFF"
    FROM new_rows
)
{% if astra_is_incremental() %},
target_latest AS (
    SELECT
        "LINK_HK",
        "HASHDIFF"
    FROM {{ this }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY "LINK_HK"
        ORDER BY "LDTS" DESC, "HASHDIFF" DESC
    ) = 1
)
{% endif %}
SELECT
    source_row."LINK_HK", source_row."HASHDIFF", source_row."PROJECT_ROLE", source_row."START_DATE", source_row."LDTS", source_row."RSRC"
FROM sequenced AS source_row
{% if astra_is_incremental() %}
LEFT JOIN target_latest AS target_row
    ON target_row."LINK_HK" = source_row."LINK_HK"
{% endif %}
WHERE (
    source_row."PREVIOUS_HASHDIFF" IS NULL
    OR source_row."PREVIOUS_HASHDIFF" <> source_row."HASHDIFF"
)
{% if astra_is_incremental() %}
  AND (
      source_row."PREVIOUS_HASHDIFF" IS NOT NULL
      OR target_row."LINK_HK" IS NULL
      OR target_row."HASHDIFF" <> source_row."HASHDIFF"
  )
{% endif %}
