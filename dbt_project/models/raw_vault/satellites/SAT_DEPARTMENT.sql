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
hashed AS (
    SELECT
        {{ astra_dv_hash(['DEPARTMENT_ID']) }} AS "HUB_HK",
        {{ astra_dv_hash(['DEPARTMENT_NAME', 'LOCATION']) }} AS "HASHDIFF",
        "DEPARTMENT_NAME", "LOCATION", "LDTS", "RSRC"
    FROM normalized
    WHERE "DEPARTMENT_ID" IS NOT NULL
), deduplicated AS (
    SELECT
        "HUB_HK",
        "HASHDIFF",
        "DEPARTMENT_NAME",
        "LOCATION",
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
        "DEPARTMENT_NAME",
        "LOCATION",
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
        "DEPARTMENT_NAME",
        "LOCATION",
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
    source_row."HUB_HK", source_row."HASHDIFF", source_row."DEPARTMENT_NAME", source_row."LOCATION", source_row."LDTS", source_row."RSRC"
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
