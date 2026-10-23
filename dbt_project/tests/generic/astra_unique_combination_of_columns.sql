{% test astra_unique_combination_of_columns(model, combination_of_columns) %}
SELECT
    {% for column_name in combination_of_columns %}{{ adapter.quote(column_name) }}{{ "," if not loop.last }}{% endfor %}
FROM {{ model }}
GROUP BY {% for column_name in combination_of_columns %}{{ adapter.quote(column_name) }}{{ "," if not loop.last }}{% endfor %}
HAVING COUNT(*) > 1
{% endtest %}
