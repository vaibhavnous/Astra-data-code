{% macro astra_dv_hash(column_names) -%}
SHA2(
    CONCAT_WS(
        '||',
        {%- for column_name in column_names %}
        COALESCE(NULLIF(UPPER(TRIM(CAST({{ adapter.quote(column_name) }} AS VARCHAR))), ''), '^^'){{ "," if not loop.last }}
        {%- endfor %}
    ),
    256
)
{%- endmacro %}
