{% macro astra_source_change_predicate(config) -%}
{% if config.get('source_change_predicate') is not none %}
    {{ return(config.get('source_change_predicate')) }}
{% endif %}
{% set committed = config.get('committed_watermark') %}
{% if committed is none %}
    {{ return('1=1') }}
{% endif %}
{% if not config.get('source_change_detected', false) %}
    {{ return('1=0') }}
{% endif %}
{% set watermark = adapter.quote(config['watermark_column']) %}
{% set boundary = "TO_TIMESTAMP_NTZ('" ~ (committed | string | replace("'", "''")) ~ "')" %}
{% set columns = config.get('tie_breaker_columns', []) %}
{% if not columns %}
    {{ return(watermark ~ ' > ' ~ boundary) }}
{% endif %}
{% set values = config.get('committed_tie_breaker_values', []) %}
{% set types = config.get('tie_breaker_types', []) %}
{% if columns | length != values | length or columns | length != types | length %}
    {{ exceptions.raise_compiler_error('Incremental tie-breaker boundary is incomplete for ' ~ config.get('watermark_column', 'unknown source')) }}
{% endif %}
{% set state = namespace(comparisons=[], equalities=[]) %}
{% for column in columns %}
    {% set literal = "CAST('" ~ (values[loop.index0] | string | replace("'", "''")) ~ "' AS " ~ types[loop.index0] ~ ")" %}
    {% set expression = adapter.quote(column) ~ ' > ' ~ literal %}
    {% if state.equalities %}
        {% set expression = '(' ~ (state.equalities | join(' AND ')) ~ ' AND ' ~ expression ~ ')' %}
    {% endif %}
    {% do state.comparisons.append(expression) %}
    {% do state.equalities.append(adapter.quote(column) ~ ' = ' ~ literal) %}
{% endfor %}
{{ return('(' ~ watermark ~ ' > ' ~ boundary ~ ' OR (' ~ watermark ~ ' = ' ~ boundary ~ ' AND (' ~ (state.comparisons | join(' OR ')) ~ ')))') }}
{%- endmacro %}

{% macro astra_source_recompute_predicate(config) -%}
{% if config.get('source_recompute_predicate') is not none %}
    {{ return(config.get('source_recompute_predicate')) }}
{% endif %}
{% if config.get('schema_reprocess') or config.get('lookup_population_reprocess') %}
    {{ return('1=1') }}
{% endif %}
{% if config.get('detection_mode') != 'WATERMARK' %}
    {{ return('1=1') }}
{% endif %}
{% set committed = config.get('committed_watermark') %}
{% set lookback = config.get('lookback_minutes', 0) | int %}
{% if committed is none or lookback <= 0 %}
    {{ return(astra_source_change_predicate(config)) }}
{% endif %}
{% set watermark = adapter.quote(config['watermark_column']) %}
{% set boundary = "TO_TIMESTAMP_NTZ('" ~ (committed | string | replace("'", "''")) ~ "')" %}
{{ return(watermark ~ " >= DATEADD('minute', -" ~ lookback ~ ", " ~ boundary ~ ")") }}
{%- endmacro %}

{% macro astra_incremental_source_predicate(source_relation) -%}
{% if var('astra_offline_compile', false) %}
    {{ return('1=1') }}
{% endif %}
{% set config = var('astra_incremental_source_configs', {}).get(source_relation) %}
{% set current = config.get('source_current_predicate', '1=1') if config is not none else '1=1' %}
{% if var('astra_processing_mode', 'FULL') != 'INCREMENTAL' or not astra_is_incremental() %}
    {{ return(current) }}
{% endif %}
{% if config is none %}
    {{ exceptions.raise_compiler_error('No approved incremental configuration for source ' ~ source_relation) }}
{% endif %}
{% if var('astra_reconciliation_active', false) %}
    {% set window = var('astra_reconciliation_windows', {}).get(source_relation) %}
    {% if window is none %}
        {{ return('1=0') }}
    {% endif %}
    {% if window.get('mode') == 'FULL_HISTORY' %}
        {{ return(current) }}
    {% endif %}
    {% set start = window['window_start'] | replace("'", "''") %}
    {% set end = window['window_end'] | replace("'", "''") %}
    {% set predicate = adapter.quote(window['date_column']) ~ " >= TO_TIMESTAMP_NTZ('" ~ start ~ "') AND " ~ adapter.quote(window['date_column']) ~ " < TO_TIMESTAMP_NTZ('" ~ end ~ "')" %}
    {{ return(predicate if current == '1=1' else '(' ~ predicate ~ ') AND (' ~ current ~ ')') }}
{% endif %}
{% if config.get('detection_mode') in ['STATIC', 'FULL'] %}
    {{ return(current) }}
{% endif %}
{% if config.get('source_recompute_predicate') is not none %}
    {% set predicate = config.get('source_recompute_predicate') %}
{% else %}
{% set predicate = astra_source_recompute_predicate(config) %}
{% endif %}
{% if predicate is none %}
    {{ exceptions.raise_compiler_error('No source-change predicate was prepared for ' ~ source_relation) }}
{% endif %}
{{ return(predicate if current == '1=1' else '(' ~ predicate ~ ') AND (' ~ current ~ ')') }}
{%- endmacro %}

{% macro astra_current_source_predicate(source_relation) -%}
{% set config = var('astra_incremental_source_configs', {}).get(source_relation, {}) %}
{% if config.get('source_current_view') %}{{ return('1=1') }}{% endif %}
{{ return(config.get('source_current_predicate', '1=1')) }}
{%- endmacro %}

{% macro astra_current_source_population(source_relation, relation) -%}
{% set config = var('astra_incremental_source_configs', {}).get(source_relation, {}) %}
{% if config.get('source_current_view') %}{{ return('(' ~ config['source_current_view'] ~ ')') }}{% endif %}
{{ return(relation) }}
{%- endmacro %}
