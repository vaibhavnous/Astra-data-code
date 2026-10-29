-- Astra Gold changes against referenced branch:
-- ADDITION VW_EMPLOYEEASSIGNMENTREPORT: The current Gold design produces a model absent from the referenced branch.
-- Evidence: metadata/astra_gold_column_change_rationale.md
SELECT
    "ENO",
    "ENAME",
    "EMAIL",
    "JOINDATE",
    "DEPARTMENT_ID",
    "LOCATION"
FROM {{ ref('FACT_EMPLOYEEASSIGNMENTREPORT') }}
