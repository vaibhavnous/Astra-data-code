WITH source_snapshot AS (
    WITH
    anchor AS (SELECT * FROM {{ ref('HUB_EMPLOYEE') }}),
    sat_1 AS (
        SELECT * FROM {{ ref('SAT_ADDRESS') }}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY "HUB_HK" ORDER BY "LDTS" DESC, "HASHDIFF" DESC
        ) = 1
    ),
    sat_2 AS (
        SELECT * FROM {{ ref('SAT_EMPLOYEE') }}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY "HUB_HK" ORDER BY "LDTS" DESC, "HASHDIFF" DESC
        ) = 1
    ),
    sat_3 AS (
        SELECT * FROM {{ ref('SAT_SALARY') }}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY "HUB_HK" ORDER BY "LDTS" DESC, "HASHDIFF" DESC
        ) = 1
    )
    SELECT
        anchor."HUB_HK" AS "HUB_HK",
        anchor."ENO" AS "ENO",
        anchor."LDTS" AS "LDTS",
        anchor."RSRC" AS "RSRC",
        sat_1."ADDRESS_LINE" AS "ADDRESS_LINE",
        sat_1."CITY" AS "CITY",
        sat_1."STATE_PROVINCE" AS "STATE_PROVINCE",
        sat_1."COUNTRY" AS "COUNTRY",
        sat_2."ENAME" AS "ENAME",
        sat_2."EMAIL" AS "EMAIL",
        sat_2."JOIN_DATE" AS "JOIN_DATE",
        sat_3."BASIC_SALARY" AS "BASIC_SALARY",
        sat_3."BONUS" AS "BONUS",
        sat_3."CURRENCY" AS "CURRENCY",
        sat_3."EFFECTIVE_DATE" AS "EFFECTIVE_DATE"
    FROM anchor
    LEFT JOIN sat_1 ON anchor."HUB_HK" = sat_1."HUB_HK"
    LEFT JOIN sat_2 ON anchor."HUB_HK" = sat_2."HUB_HK"
    LEFT JOIN sat_3 ON anchor."HUB_HK" = sat_3."HUB_HK"
), source_hashed AS (
    SELECT
        "HUB_HK",
        "ENO",
        "LDTS",
        "RSRC",
        "ADDRESS_LINE",
        "CITY",
        "STATE_PROVINCE",
        "COUNTRY",
        "ENAME",
        "EMAIL",
        "JOIN_DATE",
        "BASIC_SALARY",
        "BONUS",
        "CURRENCY",
        "EFFECTIVE_DATE",
        SHA2(
        CONCAT_WS(
            '||',
            COALESCE(NULLIF(UPPER(TRIM(CAST(source_row."HUB_HK" AS VARCHAR))), ''), '^^'),
            COALESCE(NULLIF(UPPER(TRIM(CAST(source_row."ENO" AS VARCHAR))), ''), '^^'),
            COALESCE(NULLIF(UPPER(TRIM(CAST(source_row."LDTS" AS VARCHAR))), ''), '^^'),
            COALESCE(NULLIF(UPPER(TRIM(CAST(source_row."RSRC" AS VARCHAR))), ''), '^^'),
            COALESCE(NULLIF(UPPER(TRIM(CAST(source_row."ADDRESS_LINE" AS VARCHAR))), ''), '^^'),
            COALESCE(NULLIF(UPPER(TRIM(CAST(source_row."CITY" AS VARCHAR))), ''), '^^'),
            COALESCE(NULLIF(UPPER(TRIM(CAST(source_row."STATE_PROVINCE" AS VARCHAR))), ''), '^^'),
            COALESCE(NULLIF(UPPER(TRIM(CAST(source_row."COUNTRY" AS VARCHAR))), ''), '^^'),
            COALESCE(NULLIF(UPPER(TRIM(CAST(source_row."ENAME" AS VARCHAR))), ''), '^^'),
            COALESCE(NULLIF(UPPER(TRIM(CAST(source_row."EMAIL" AS VARCHAR))), ''), '^^'),
            COALESCE(NULLIF(UPPER(TRIM(CAST(source_row."JOIN_DATE" AS VARCHAR))), ''), '^^'),
            COALESCE(NULLIF(UPPER(TRIM(CAST(source_row."BASIC_SALARY" AS VARCHAR))), ''), '^^'),
            COALESCE(NULLIF(UPPER(TRIM(CAST(source_row."BONUS" AS VARCHAR))), ''), '^^'),
            COALESCE(NULLIF(UPPER(TRIM(CAST(source_row."CURRENCY" AS VARCHAR))), ''), '^^'),
            COALESCE(NULLIF(UPPER(TRIM(CAST(source_row."EFFECTIVE_DATE" AS VARCHAR))), ''), '^^')
        ),
        256
    ) AS "astra_record_hash"
    FROM source_snapshot AS source_row
    WHERE "HUB_HK" IS NOT NULL
), source_deduplicated AS (
    SELECT
        "HUB_HK",
        "ENO",
        "LDTS",
        "RSRC",
        "ADDRESS_LINE",
        "CITY",
        "STATE_PROVINCE",
        "COUNTRY",
        "ENAME",
        "EMAIL",
        "JOIN_DATE",
        "BASIC_SALARY",
        "BONUS",
        "CURRENCY",
        "EFFECTIVE_DATE",
        "astra_record_hash"
    FROM source_hashed
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY "HUB_HK"
        ORDER BY "astra_record_hash" DESC
    ) = 1
), source_versions AS (
    SELECT
        source_row."HUB_HK",
        source_row."ENO",
        source_row."LDTS",
        source_row."RSRC",
        source_row."ADDRESS_LINE",
        source_row."CITY",
        source_row."STATE_PROVINCE",
        source_row."COUNTRY",
        source_row."ENAME",
        source_row."EMAIL",
        source_row."JOIN_DATE",
        source_row."BASIC_SALARY",
        source_row."BONUS",
        source_row."CURRENCY",
        source_row."EFFECTIVE_DATE",
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
        target_row."ENO",
        target_row."LDTS",
        target_row."RSRC",
        target_row."ADDRESS_LINE",
        target_row."CITY",
        target_row."STATE_PROVINCE",
        target_row."COUNTRY",
        target_row."ENAME",
        target_row."EMAIL",
        target_row."JOIN_DATE",
        target_row."BASIC_SALARY",
        target_row."BONUS",
        target_row."CURRENCY",
        target_row."EFFECTIVE_DATE",
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
    "ENO",
    "LDTS",
    "RSRC",
    "ADDRESS_LINE",
    "CITY",
    "STATE_PROVINCE",
    "COUNTRY",
    "ENAME",
    "EMAIL",
    "JOIN_DATE",
    "BASIC_SALARY",
    "BONUS",
    "CURRENCY",
    "EFFECTIVE_DATE",
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
    "ENO",
    "LDTS",
    "RSRC",
    "ADDRESS_LINE",
    "CITY",
    "STATE_PROVINCE",
    "COUNTRY",
    "ENAME",
    "EMAIL",
    "JOIN_DATE",
    "BASIC_SALARY",
    "BONUS",
    "CURRENCY",
    "EFFECTIVE_DATE",
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
    "ENO",
    "LDTS",
    "RSRC",
    "ADDRESS_LINE",
    "CITY",
    "STATE_PROVINCE",
    "COUNTRY",
    "ENAME",
    "EMAIL",
    "JOIN_DATE",
    "BASIC_SALARY",
    "BONUS",
    "CURRENCY",
    "EFFECTIVE_DATE",
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
