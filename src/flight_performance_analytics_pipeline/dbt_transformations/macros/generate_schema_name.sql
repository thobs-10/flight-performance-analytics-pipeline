{#
  generate_schema_name.sql
  ========================
  Override dbt's default schema naming behaviour.

  By default dbt prefixes custom schema names with the target schema, producing
  names like "staging_marts" when the target is "staging" and +schema is "marts".
  This macro removes that prefix so the schema name is used exactly as declared
  in dbt_project.yml:

    +schema: marts     → writes to the "marts" schema
    +schema: analytics → writes to the "analytics" schema
    (no +schema)       → falls back to the target schema ("staging")

  See: https://docs.getdbt.com/docs/build/custom-schemas
#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
