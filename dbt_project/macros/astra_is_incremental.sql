{% macro astra_is_incremental() -%}
{% if var('astra_force_incremental', false) %}
    {{ return(true) }}
{% endif %}
{% if var('astra_offline_compile', false) %}
    {{ return(false) }}
{% endif %}
{{ return(is_incremental()) }}
{%- endmacro %}
