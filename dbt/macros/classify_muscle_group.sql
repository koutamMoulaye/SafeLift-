{#
    HEURISTIQUE DE CLASSIFICATION PAR MOTS-CLES -- PAS UNE TAXONOMIE MEDICALE
    VALIDEE. Aucune des tables sources (Bronze/Silver) ne fournit de colonne
    muscle_group : 600k_fitness_detailed ne decrit que exercise_name, sans
    aucun attribut anatomique. Ce mapping est donc une regle de gestion
    inventee pour ce projet, a base de correspondance de sous-chaines sur le
    nom d'exercice normalise (voir normalize_exercise_name.sql). Elle est
    volontairement simple et 100% transparente (une seule chaine CASE WHEN,
    aucune ML, aucune boite noire) pour rester auditable par un jury, mais
    n'a AUCUNE valeur scientifique : deux exercices au nom proche peuvent
    solliciter des groupes musculaires differents dans la realite, et
    inversement. Voir data/gold/GOLD_MODEL_DECISIONS.md pour la liste
    complete des mots-cles et leurs limites.

    L'ORDRE des WHEN compte : les regles les plus specifiques sont placees
    avant les plus generiques pour eviter les faux positifs (ex. "romanian
    deadlift" doit matcher la regle "deadlift" -> lower_back avant toute
    regle generique sur "leg").
#}
{% macro classify_muscle_group(normalized_name_column) -%}
    case
        -- lower_back : mouvements de hinge / extension lombaire
        when {{ normalized_name_column }} like '%deadlift%'
          or {{ normalized_name_column }} like '%good morning%'
          or {{ normalized_name_column }} like '%hyperextension%'
          or {{ normalized_name_column }} like '%back extension%'
            then 'lower_back'

        -- knee : mouvements de flexion/extension du genou en charge
        when {{ normalized_name_column }} like '%squat%'
          or {{ normalized_name_column }} like '%lunge%'
          or {{ normalized_name_column }} like '%leg press%'
          or {{ normalized_name_column }} like '%leg extension%'
          or {{ normalized_name_column }} like '%step up%'
            then 'knee'

        -- shoulder : presses/elevations d'epaule
        when {{ normalized_name_column }} like '%overhead press%'
          or {{ normalized_name_column }} like '%shoulder press%'
          or {{ normalized_name_column }} like '%military press%'
          or {{ normalized_name_column }} like '%arnold press%'
          or {{ normalized_name_column }} like '%lateral raise%'
          or {{ normalized_name_column }} like '%front raise%'
          or {{ normalized_name_column }} like '%upright row%'
          or {{ normalized_name_column }} like '%shrug%'
            then 'shoulder'

        -- chest : presses/ecartes pectoraux
        when {{ normalized_name_column }} like '%bench press%'
          or {{ normalized_name_column }} like '%chest press%'
          or {{ normalized_name_column }} like '%chest fly%'
          or {{ normalized_name_column }} like '% fly%'
          or {{ normalized_name_column }} like 'fly%'
          or {{ normalized_name_column }} like '%push up%'
          or {{ normalized_name_column }} like '%pushup%'
          or {{ normalized_name_column }} like '%dip%'
            then 'chest'

        -- back : tirages dorsaux (haut du dos)
        when {{ normalized_name_column }} like '%pull up%'
          or {{ normalized_name_column }} like '%pullup%'
          or {{ normalized_name_column }} like '%chin up%'
          or {{ normalized_name_column }} like '%pulldown%'
          or {{ normalized_name_column }} like '%pull down%'
          or {{ normalized_name_column }} like '%row%'
          or {{ normalized_name_column }} like '%pullover%'
          or {{ normalized_name_column }} like '%pull over%'
            then 'back'

        -- arms : biceps/triceps isoles
        when {{ normalized_name_column }} like '%curl%'
          or {{ normalized_name_column }} like '%tricep%'
          or {{ normalized_name_column }} like '%skull crusher%'
          or {{ normalized_name_column }} like '%close grip%'
            then 'arms'

        -- abs : sangle abdominale
        when {{ normalized_name_column }} like '%crunch%'
          or {{ normalized_name_column }} like '%sit up%'
          or {{ normalized_name_column }} like '%situp%'
          or {{ normalized_name_column }} like '%plank%'
          or {{ normalized_name_column }} like '%russian twist%'
          or {{ normalized_name_column }} like '%ab wheel%'
          or {{ normalized_name_column }} like '%leg raise%'
            then 'abs'

        -- legs (hors genou) : ischio-jambiers, mollets, fessiers, hanche
        when {{ normalized_name_column }} like '%calf raise%'
          or {{ normalized_name_column }} like '%hip thrust%'
          or {{ normalized_name_column }} like '%glute bridge%'
          or {{ normalized_name_column }} like '%leg curl%'
          or {{ normalized_name_column }} like '%hamstring%'
          or {{ normalized_name_column }} like '%hip abduction%'
          or {{ normalized_name_column }} like '%hip adduction%'
            then 'legs'

        else 'unknown'
    end
{%- endmacro %}
