WITH source_snapshot AS (
    WITH
    anchor AS (SELECT * FROM {{ ref('HUB_DEPARTMENT') }}),
    sat_1 AS (
        SELECT * FROM {{ ref('SAT_DEPARTMENT') }}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY "HUB_HK" ORDER BY "LDTS" DESC, "HASHDIFF" DESC
        ) = 1
    )
    SELECT
        anchor."HUB_HK" AS "HUB_HK",
        anchor."DEPARTMENT_ID" AS "DEPARTMENT_ID",
        anchor."LDTS" AS "LDTS",
        anchor."RSRC" AS "RSRC",
        sat_1."DEPARTMENT_NAME" AS "DEPARTMENT_NAME",
        sat_1."LOCATION" AS "LOCATION"
    FROM anchor
    LEFT JOIN sat_1 ON anchor."HUB_HK" = sat_1."HUB_HK"
), source_hashed AS (
    SELECT
        "HUB_HK",
        "DEPARTMENT_ID",
        "LDTS",
        "RSRC",
        "DEPARTMENT_NAME",
        "LOCATION",
        SHA2(
        CONCAT_WS(
            '||',
            COALESCE(NULLIF(UPPER(TRIM(CAST(source_row."HUB_HK" AS VARCHAR))), ''), '^^'),
            COALESCE(NULLIF(UPPER(TRIM(CAST(source_row."DEPARTMENT_ID" AS VARCHAR))), ''), '^^'),
            COALESCE(NULLIF(UPPER(TRIM(CAST(source_row."LDTS" AS VARCHAR))), ''), '^^'),
            COALESCE(NULLIF(UPPER(TRIM(CAST(source_row."RSRC" AS VARCHAR))), ''), '^^'),
            COALESCE(NULLIF(UPPER(TRIM(CAST(source_row."DEPARTMENT_NAME" AS VARCHAR))), ''), '^^'),
            COALESCE(NULLIF(UPPER(TRIM(CAST(source_row."LOCATION" AS VARCHAR))), ''), '^^')
        ),
        256
    ) AS "astra_record_hash"
    FROM source_snapshot AS source_row
    WHERE "HUB_HK" IS NOT NULL
), source_deduplicated AS (
    SELECT
        "HUB_HK",
        "DEPARTMENT_ID",
        "LDTS",
        "RSRC",
        "DEPARTMENT_NAME",
        "LOCATION",
        "astra_record_hash"
    FROM source_hashed
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY "HUB_HK"
        ORDER BY "astra_record_hash" DESC
    ) = 1
), source_versions AS (
    SELECT
        source_row."HUB_HK",
        source_row."DEPARTMENT_ID",
        source_row."LDTS",
        source_row."RSRC",
        source_row."DEPARTMENT_NAME",
        source_row."LOCATION",
        SHA2(
        CONCAT_WS(
            '||',
            COALESCE(NULLIF(UPPER(TRIM(CAST(source_row."HUB_HK" AS VARCHAR))), ''), '^^'),
            COALESCE(NULLIF(UPPER(TRIM(CAST(source_row."astra_record_hash" AS VARCHAR))), ''), '^^'),
            COALESCE(NULLIF(UPPER(TRIM(CAST(CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS VARCHAR))), ''), '^^')
        ),
        256
    ) AS "astra_scd2_key",
        source_row."astra_record_hash",
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS "astra_valid_from",
        CAST(NULL AS TIMESTAMP_NTZ) AS "astra_valid_to",
        TRUE AS "astra_is_current",
        CAST('ASTRA_DATA' AS VARCHAR) AS "created_by",
        CAST('ASTRA_DATA' AS VARCHAR) AS "modified_by",
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS "created_date",
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS "modified_date"
    FROM source_deduplicated AS source_row
)
{% if astra_is_incremental() %}, current_target AS (
    SELECT *
    FROM {{ this }}
    WHERE "astra_is_current" = TRUE
), expired_versions AS (
    SELECT
        target_row."HUB_HK",
        target_row."DEPARTMENT_ID",
        target_row."LDTS",
        target_row."RSRC",
        target_row."DEPARTMENT_NAME",
        target_row."LOCATION",
        target_row."astra_scd2_key",
        target_row."astra_record_hash",
        target_row."astra_valid_from",
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS "astra_valid_to",
        FALSE AS "astra_is_current",
        target_row."created_by",
        CAST('ASTRA_DATA' AS VARCHAR) AS "modified_by",
        target_row."created_date",
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS "modified_date"
    FROM current_target AS target_row
    INNER JOIN source_deduplicated AS source_row
        ON target_row."HUB_HK" = source_row."HUB_HK"
    WHERE target_row."astra_record_hash" <> source_row."astra_record_hash"
), new_versions AS (
    SELECT source_version.*
    FROM source_versions AS source_version
    LEFT JOIN current_target AS target_row
        ON target_row."HUB_HK" = source_version."HUB_HK"
    WHERE target_row."astra_scd2_key" IS NULL
       OR target_row."astra_record_hash" <> source_version."astra_record_hash"
)
SELECT
    "HUB_HK",
    "DEPARTMENT_ID",
    "LDTS",
    "RSRC",
    "DEPARTMENT_NAME",
    "LOCATION",
    "astra_scd2_key",
    "astra_record_hash",
    "astra_valid_from",
    "astra_valid_to",
    "astra_is_current",
    "created_by",
    "modified_by",
    "created_date",
    "modified_date"
FROM expired_versions
UNION ALL
SELECT
    "HUB_HK",
    "DEPARTMENT_ID",
    "LDTS",
    "RSRC",
    "DEPARTMENT_NAME",
    "LOCATION",
    "astra_scd2_key",
    "astra_record_hash",
    "astra_valid_from",
    "astra_valid_to",
    "astra_is_current",
    "created_by",
    "modified_by",
    "created_date",
    "modified_date"
FROM new_versions
{% else %}
SELECT
    "HUB_HK",
    "DEPARTMENT_ID",
    "LDTS",
    "RSRC",
    "DEPARTMENT_NAME",
    "LOCATION",
    "astra_scd2_key",
    "astra_record_hash",
    "astra_valid_from",
    "astra_valid_to",
    "astra_is_current",
    "created_by",
    "modified_by",
    "created_date",
    "modified_date"
FROM source_versions
{% endif %}
