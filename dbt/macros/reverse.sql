{# Batch number of a reverse row: consecutive blocks of `size` rows in key order, from 0. #}
{% macro batch_no(keys, size) -%}
toInt32(intDiv(row_number() OVER (ORDER BY {{ keys }}) - 1, {{ size }}))
{%- endmacro %}

{# Hash of the target columns as written to the OLTP, as hex text (stage column row_hash). #}
{% macro row_hash(cols) -%}lower(hex(cityHash64({{ cols }}))){%- endmacro %}
