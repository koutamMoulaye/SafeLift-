{#
    Normalisation "etendue" pour le matching exercise_name (etape 2/3 du
    pipeline de matching, apres l'egalite stricte sur normalize_exercise_name).

    Contrairement a normalize_exercise_name (minuscule + ponctuation), celle-ci
    retire EN PLUS les mots d'equipement generiques (barbell, dumbbell,
    machine, cable, kettlebell, band, smith, weighted) et gere un pluriel
    simple ("curls" -> "curl"). Objectif : faire matcher "Incline Bench Press"
    (weight_training) avec "Incline Bench Press (Barbell)" (catalogue), qui
    designent le meme mouvement avec un materiel different precise ou non.

    Limite assumee (documentee dans data/gold/GOLD_MODEL_DECISIONS.md) :
    en supprimant l'equipement, on perd volontairement la distinction entre
    variantes materiel d'un meme mouvement (ex. Incline Press Barbell vs
    Incline Press Dumbbell deviennent identiques apres cette normalisation) --
    acceptable pour le matching, PAS pour un usage ou l'equipement compte.

    Pluriel : retrait d'un 's' final isole (pas 'ss', pour ne pas casser des
    mots comme "press"/"dips" légitimement termines par 2+ consonnes) --
    heuristique simple, pas un vrai lemmatiseur.
#}
{% macro normalize_exercise_base_name(column_name) -%}
    trim(
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    {{ normalize_exercise_name(column_name) }},
                    '\y(barbell|dumbbells?|machine|cable|kettlebell|bands?|smith|weighted|bodyweight)\y', '', 'g'
                ),
                '\y([a-z]*[^s])s\y', '\1', 'g'  -- pluriel simple : "curls" -> "curl" ("press"/"dips" se terminent en "ss" -> inchanges, cf. commentaire ci-dessus)
            ),
            '\s+', ' ', 'g'
        )
    )
{%- endmacro %}
