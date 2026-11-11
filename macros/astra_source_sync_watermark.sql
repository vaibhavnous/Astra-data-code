{% macro astra_source_sync_watermark() -%}
{% if var('astra_offline_compile', false) %}
    {{ return('CAST(NULL AS TIMESTAMP_NTZ)') }}
{% endif %}
{% set context = var('astra_source_sync_context', {}) %}
{% if context.get('mode') != 'WATERMARK' %}
    {{ return('CAST(NULL AS TIMESTAMP_NTZ)') }}
{% endif %}
{% set watermark_literals = [] %}
{% for value in context.get('watermarks', []) %}
    {% do watermark_literals.append("TO_TIMESTAMP_NTZ('" ~ (value | string | replace("'", "''")) ~ "')") %}
{% endfor %}
{% if watermark_literals | length == 0 %}
    {{ exceptions.raise_compiler_error('No safe source-sync watermark was prepared for this Gold run') }}
{% endif %}
{{ return('LEAST(' ~ (watermark_literals | join(', ')) ~ ')') }}
{%- endmacro %}
