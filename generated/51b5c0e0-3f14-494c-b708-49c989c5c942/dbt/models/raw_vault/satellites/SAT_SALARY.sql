WITH normalized AS (
    SELECT
            CAST("ENO" AS NUMBER(38,0)) AS "ENO",
            CAST("BASIC_SALARY" AS NUMBER(18,2)) AS "BASIC_SALARY",
            CAST("BONUS" AS NUMBER(18,2)) AS "BONUS",
            NULLIF(TRIM(CAST("CURRENCY" AS VARCHAR(134217728))), '') AS "CURRENCY",
            TRY_TO_DATE("EFFECTIVE_DATE") AS "EFFECTIVE_DATE",
            TO_TIMESTAMP_NTZ('{{ env_var("DBT_ATHENA_DV_LOAD_TIMESTAMP") }}') AS "LDTS",
            'BRONZE.SALARY' AS "RSRC"
        FROM {{ source('bronze_bronze', 'SALARY') }}
),
hashed AS (
    SELECT
        {{ astra_dv_hash(['ENO']) }} AS "HUB_HK",
        {{ astra_dv_hash(['BASIC_SALARY', 'BONUS', 'CURRENCY', 'EFFECTIVE_DATE']) }} AS "HASHDIFF",
        "BASIC_SALARY", "BONUS", "CURRENCY", "EFFECTIVE_DATE", "LDTS", "RSRC"
    FROM normalized
    WHERE "ENO" IS NOT NULL
), deduplicated AS (
    SELECT
        "HUB_HK",
        "HASHDIFF",
        "BASIC_SALARY",
        "BONUS",
        "CURRENCY",
        "EFFECTIVE_DATE",
        "LDTS",
        "RSRC"
    FROM hashed
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY "HUB_HK", "LDTS"
        ORDER BY "HASHDIFF", "RSRC"
    ) = 1
), new_rows AS (
    SELECT
        "HUB_HK",
        "HASHDIFF",
        "BASIC_SALARY",
        "BONUS",
        "CURRENCY",
        "EFFECTIVE_DATE",
        "LDTS",
        "RSRC"
    FROM deduplicated AS source_row
    {% if astra_is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }} AS existing
        WHERE existing."HUB_HK" = source_row."HUB_HK"
          AND existing."LDTS" = source_row."LDTS"
    )
    {% endif %}
), sequenced AS (
    SELECT
        "HUB_HK",
        "HASHDIFF",
        "BASIC_SALARY",
        "BONUS",
        "CURRENCY",
        "EFFECTIVE_DATE",
        "LDTS",
        "RSRC",
        LAG("HASHDIFF") OVER (
            PARTITION BY "HUB_HK"
            ORDER BY "LDTS", "HASHDIFF"
        ) AS "PREVIOUS_HASHDIFF"
    FROM new_rows
)
{% if astra_is_incremental() %},
target_latest AS (
    SELECT
        "HUB_HK",
        "HASHDIFF"
    FROM {{ this }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY "HUB_HK"
        ORDER BY "LDTS" DESC, "HASHDIFF" DESC
    ) = 1
)
{% endif %}
SELECT
    source_row."HUB_HK", source_row."HASHDIFF", source_row."BASIC_SALARY", source_row."BONUS", source_row."CURRENCY", source_row."EFFECTIVE_DATE", source_row."LDTS", source_row."RSRC"
FROM sequenced AS source_row
{% if astra_is_incremental() %}
LEFT JOIN target_latest AS target_row
    ON target_row."HUB_HK" = source_row."HUB_HK"
{% endif %}
WHERE (
    source_row."PREVIOUS_HASHDIFF" IS NULL
    OR source_row."PREVIOUS_HASHDIFF" <> source_row."HASHDIFF"
)
{% if astra_is_incremental() %}
  AND (
      source_row."PREVIOUS_HASHDIFF" IS NOT NULL
      OR target_row."HUB_HK" IS NULL
      OR target_row."HASHDIFF" <> source_row."HASHDIFF"
  )
{% endif %}
