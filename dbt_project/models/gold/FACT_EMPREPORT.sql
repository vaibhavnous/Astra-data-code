SELECT
    base_dim."ENO" AS "ENO",
    base_dim."ENAME" AS "ENAME",
    base_dim."EMAIL" AS "EMAIL",
    base_dim."CITY" AS "CITY",
    base_dim."COUNTRY" AS "COUNTRY",
    dimension_1."DEPARTMENT_NAME" AS "DEPARTMENT_NAME",
    relationship_1."MANAGER_NAME" AS "MANAGER_NAME",
    base_dim."BASIC_SALARY" AS "BASIC_SALARY",
    base_dim."CURRENCY" AS "CURRENCY",
    dimension_2."PROJECT_NAME" AS "PROJECT_NAME",
    relationship_2."PROJECT_ROLE" AS "PROJECT_ROLE",
    CAST('ASTRA_DATA' AS VARCHAR) AS "created_by",
    CAST('ASTRA_DATA' AS VARCHAR) AS "modified_by",
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS "created_date",
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS "modified_date"
FROM {{ ref('DIM_EMPLOYEE') }} AS base_dim
LEFT JOIN {{ ref('FACT_EMPLOYEE_DEPARTMENT') }} AS relationship_1 ON base_dim."HUB_HK" = relationship_1."EMPLOYEE_HUB_HK"
LEFT JOIN {{ ref('FACT_EMPLOYEE_PROJECT') }} AS relationship_2 ON base_dim."HUB_HK" = relationship_2."EMPLOYEE_HUB_HK"
LEFT JOIN {{ ref('DIM_DEPARTMENT') }} AS dimension_1 ON relationship_1."DEPARTMENT_HUB_HK" = dimension_1."HUB_HK" AND dimension_1."astra_is_current" = TRUE
LEFT JOIN {{ ref('DIM_PROJECT') }} AS dimension_2 ON relationship_2."PROJECT_HUB_HK" = dimension_2."HUB_HK" AND dimension_2."astra_is_current" = TRUE
WHERE base_dim."astra_is_current" = TRUE
