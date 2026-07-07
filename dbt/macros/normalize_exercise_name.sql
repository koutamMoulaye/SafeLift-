{#
    Normalisation d'un nom d'exercice pour le matching entre
    weight_training.exercise_name (seances reellement loggees) et
    600k_fitness_detailed.exercise_name (catalogue de reference) :
    minuscules, ponctuation retiree, espaces multiples reduits a un seul.
    Ex: "Incline Bench Press (Barbell)" -> "incline bench press barbell".

    Limite assumee et documentee (voir data/gold/GOLD_MODEL_DECISIONS.md) :
    c'est un matching textuel approximatif, pas une table de correspondance
    validee manuellement -- le taux de matching reel est mesure et publie,
    pas suppose parfait.
#}
{% macro normalize_exercise_name(column_name) -%}
    trim(
        regexp_replace(
            regexp_replace(lower({{ column_name }}), '[^a-z0-9]+', ' ', 'g'),
            '\s+', ' ', 'g'
        )
    )
{%- endmacro %}
