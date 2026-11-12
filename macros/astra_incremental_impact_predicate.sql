{% macro astra_incremental_impact_predicate(target_model, grain_columns, relation_alias) -%}
{% if astra_affected_plan is defined and astra_affected_plan(target_model) is not none %}
    {{ return('1=1') }}
{% endif %}
{% if var('astra_processing_mode', 'FULL') != 'INCREMENTAL' or not astra_is_incremental() %}
    {{ return('1=1') }}
{% endif %}
{% if var('astra_reconciliation_active', false) %}
    {# Historical keys may not have a current Bronze impact recipe. Compare
       the complete rebuilt Silver-derived Gold result with the current target. #}
    {{ return('1=1') }}
{% endif %}
{% if var('astra_incremental_target_strategies', {}).get(target_model) == 'FULL_RECOMPUTE' %}
    {{ return('1=1') }}
{% endif %}
{% set context = var('astra_incremental_impact_context', {}) %}
{% if context.get('run_id') is none or context.get('database') is none %}
    {{ exceptions.raise_compiler_error('No incremental Gold impact context was prepared for ' ~ target_model) }}
{% endif %}
{% set key_parts = [] %}
{% for column_name in grain_columns %}
    {% do key_parts.append("'" ~ column_name ~ "', CAST(" ~ relation_alias ~ "." ~ adapter.quote(column_name) ~ " AS VARCHAR)") %}
{% endfor %}
{% set run_id = (context.get('run_id') | string | replace("'", "''")) %}
{% set safe_target_model = (target_model | string | replace("'", "''")) %}
{% set prior_predicate = (
    "EXISTS (SELECT 1 FROM " ~ adapter.quote(context.get('database')) ~ ".METADATA.CTL_OUTPUT_IMPACT_KEY AS impact_key " ~
    "WHERE impact_key.RUN_ID = '" ~ run_id ~ "' " ~
    "AND impact_key.TARGET_MODEL = '" ~ safe_target_model ~ "' " ~
    "AND (impact_key.IMPACT_TYPE = 'FULL_RECOMPUTE' OR impact_key.BUSINESS_GRAIN_KEY_JSON = TO_JSON(OBJECT_CONSTRUCT(" ~ (key_parts | join(', ')) ~ "))))"
) %}
{% set predicates = [prior_predicate] %}
{% for query in var('astra_incremental_join_queries', {}).get(target_model, []) %}
    {% do predicates.append("EXISTS (SELECT 1 FROM (" ~ query ~ ") AS joined_impact WHERE joined_impact.business_grain_key_json = TO_JSON(OBJECT_CONSTRUCT(" ~ (key_parts | join(', ')) ~ ")))") %}
{% endfor %}
{{ return('(' ~ (predicates | join(' OR ')) ~ ')') }}
{%- endmacro %}
