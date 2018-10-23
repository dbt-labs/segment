{%- macro surrogate_key() -%}

{% set fields = [] %}

{%- for field in varargs -%}

    {% set _ = fields.append(
        "coalesce(cast(" ~ field ~ " as " ~ segment.type_string() ~ "), '')"
    ) %}

    {% if not loop.last %}
        {% set _ = fields.append("'-'") %}
    {% endif %}

{%- endfor -%}

{{segment.hash(segment.concat(fields))}}

{%- endmacro -%}