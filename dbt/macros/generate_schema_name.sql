{#
    Par defaut, dbt concatene le schema de la target (ici "gold") avec le
    custom_schema declare sur chaque dossier de modeles (dbt_project.yml),
    donnant par exemple "gold_staging". On surcharge cette macro pour que
    les modeles staging/ atterrissent exactement dans le schema "staging"
    et les modeles marts/ exactement dans "gold" -- plus lisible pour
    explorer la base (psql \dn) et pour brancher un futur outil de BI/API.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
