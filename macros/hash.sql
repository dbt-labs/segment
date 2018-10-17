{% macro hash(field) -%}
  {{ adapter_macro('segment.hash', field) }}
{%- endmacro %}


{% macro default__hash(field) -%}
    md5(cast({{field}} as {{segment.type_string()}}))
{%- endmacro %}


{% macro bigquery__hash(field) -%}
    to_hex({{segment.default__hash(field)}})
{%- endmacro %}