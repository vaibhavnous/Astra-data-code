-- Astra Gold changes against referenced branch:
-- ADDITION FACT_EMPLOYEEASSIGNMENTREPORT: The current Gold design produces a model absent from the referenced branch.
-- Evidence: metadata/astra_gold_column_change_rationale.md
SELECT
    base_dim."ENO" AS "ENO",
    base_dim."ENAME" AS "ENAME",
    base_dim."EMAIL" AS "EMAIL",
    base_dim."JOIN_DATE" AS "JOINDATE",
    dimension_1."DEPARTMENT_ID" AS "DEPARTMENT_ID",
    dimension_1."LOCATION" AS "LOCATION",
    CAST('ASTRA_DATA' AS VARCHAR) AS "created_by",
    CAST('ASTRA_DATA' AS VARCHAR) AS "modified_by",
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS "created_date",
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS "modified_date"
FROM {{ ref('DIM_EMPLOYEE') }} AS base_dim
LEFT JOIN {{ ref('FACT_EMPLOYEE_DEPARTMENT') }} AS relationship_1 ON base_dim."HUB_HK" = relationship_1."EMPLOYEE_HUB_HK"
LEFT JOIN {{ ref('DIM_DEPARTMENT') }} AS dimension_1 ON relationship_1."DEPARTMENT_HUB_HK" = dimension_1."HUB_HK" AND dimension_1."astra_is_current" = TRUE
WHERE base_dim."astra_is_current" = TRUE
