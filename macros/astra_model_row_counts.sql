{% macro astra_record_model_row_counts() -%}
{% if execute and not var('astra_offline_compile', false) and flags.WHICH in ['run', 'build'] %}
{% for result in results %}
{% if result.node.resource_type == 'model' %}
{% set response = result.adapter_response or {} %}
{% do log('ASTRA_MODEL_ROW_COUNT|' ~ result.node.unique_id ~ '|' ~ result.node.name ~ '|' ~ result.node.config.materialized ~ '|' ~ result.status ~ '|' ~ (response.get('query_id') or ''), info=true) %}
{% endif %}
{% endfor %}
{% endif %}
{{ return('SELECT 1') }}
{%- endmacro %}
