{#
  Primary reason of a zone price (PLAN 8.1): a hold first; an unchanged row has none; otherwise
  the binding constraint in precedence MARGIN_FLOOR > FLOOR_LIMITED_BY_CAP > CAPPED >
  LADDER_ADJUST, else the largest component, else ENDING_ROUND (only the rounding moved it).
#}
{% macro primary_reason(hold_reason, reasons, top_component, changed) -%}
multiIf({{ hold_reason }} != '', {{ hold_reason }},
        not {{ changed }}, NULL,
        has({{ reasons }}, 'MARGIN_FLOOR'), 'MARGIN_FLOOR',
        has({{ reasons }}, 'FLOOR_LIMITED_BY_CAP'), 'FLOOR_LIMITED_BY_CAP',
        has({{ reasons }}, 'CAPPED'), 'CAPPED',
        has({{ reasons }}, 'LADDER_ADJUST'), 'LADDER_ADJUST',
        {{ top_component }} != '', {{ top_component }},
        'ENDING_ROUND')
{%- endmacro %}
