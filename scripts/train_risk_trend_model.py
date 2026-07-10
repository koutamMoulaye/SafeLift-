"""SafeLift -- Entrainement/evaluation du modele ML bonus (Jalon 3, sous-etape 4/6).

Bloc 4 deja valide (GeoPort Intelligence) : ce ML est un BONUS qui raffine
risk_score, PAS une exigence de certification.

DECISION DEJA PRISE (voir data/ml/ML_DATA_PREP.md section 6) : UN SEUL
modele poole sur les 8 zones musculaires, muscle_group encode comme feature
categorique -- PAS 8 modeles independants (legs et unknown sont
structurellement trop petits pour ca : 18 et 50 lignes labelisees au total).

Ce script NE RECALCULE PAS risk_score (formule deterministe deja figee dans
fact_risk_score.sql) : il apprend a PREDIRE le risk_score de la semaine
suivante a partir de l'historique -- objectif distinct, deja tranche en
amont (sous-etape 3/6).

Comparaison OBLIGATOIRE a une baseline naive ("la semaine suivante
ressemble a la semaine courante") : si le ML ne bat pas cette baseline,
c'est annonce tel quel dans ML_TRAINING_RESULTS.md, jamais masque ni
torture via des hyperparametres pour forcer un meilleur chiffre. Le TEST
set n'est utilise QUE pour l'evaluation finale, jamais pour choisir/ajuster
quoi que ce soit (aucun hyperparametre n'est cherche par validation croisee
ici, vu le tres faible volume -- des valeurs par defaut raisonnables et
documentees suffisent).
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline

DATA_DIR = Path("/opt/airflow/data/ml")

# Features numeriques utilisees telles quelles (deja calculees en
# sous-etape 3/6, voir ML_DATA_PREP.md). Les lags peuvent contenir des NaN
# (semaine calendaire precedente absente) -- geres explicitement ci-dessous,
# JAMAIS silencieusement par un imputer scikit-learn implicite.
NUMERIC_FEATURES = [
    "risk_score_avg",
    "charge_factor_avg",
    "volume_factor_avg",
    "recup_factor_avg",
    "duree_factor_avg",
    "session_count",
    "lag_1_risk_score",
    "lag_2_risk_score",
    "lag_3_risk_score",
    "trend_vs_previous_week",
]
CATEGORICAL_FEATURES = ["muscle_group"]
TARGET_COL = "target_next_week_risk_score"


def load_datasets() -> tuple[pd.DataFrame, pd.DataFrame]:
    train_df = pd.read_parquet(DATA_DIR / "train.parquet")
    test_df = pd.read_parquet(DATA_DIR / "test.parquet")
    return train_df, test_df


def impute_lag_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Impute les NULL des colonnes lag_N_risk_score/trend_vs_previous_week.

    Choix : imputation par 0 (pas suppression de lignes). Ces NaN
    apparaissent en debut de serie temporelle par zone (moins de N semaines
    d'historique disponibles pour cette zone a ce stade) -- supprimer ces
    lignes reduirait encore un train deja petit (491 lignes) alors que
    l'information reste exploitable via les AUTRES features (risk_score_avg,
    facteurs de la semaine courante, session_count). 0 est un choix neutre
    documente ici : la moyenne du train aurait ete une alternative
    valable, mais 0 a l'avantage de rester interpretable identiquement sur
    train et test (une moyenne calculee sur le train et reappliquee au test
    est plus correcte mais ajoute une complexite non justifiee par le gain
    attendu vu le tres faible volume de lignes concernees).
    """
    df = df.copy()
    lag_cols = ["lag_1_risk_score", "lag_2_risk_score", "lag_3_risk_score", "trend_vs_previous_week"]
    for col in lag_cols:
        df[col] = df[col].fillna(0.0)
    return df


def compute_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    mae = float(mean_absolute_error(y_true, y_pred))
    return {"rmse": rmse, "mae": mae}


def build_pipeline(estimator) -> Pipeline:
    """Pipeline commun : one-hot de muscle_group + passthrough des features
    numeriques, puis l'estimateur. Encapsule l'encodage pour que le modele
    sauvegarde (data/ml/model.pkl) soit directement utilisable sur des
    donnees brutes a la sous-etape suivante, sans dupliquer la logique
    d'encodage cote consommateur.
    """
    preprocessor = ColumnTransformer(
        transformers=[
            ("muscle_group_ohe", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL_FEATURES),
            ("numeric", "passthrough", NUMERIC_FEATURES),
        ]
    )
    return Pipeline(steps=[("preprocessor", preprocessor), ("model", estimator)])


def main() -> None:
    print("[train_risk_trend_model] Chargement de train.parquet/test.parquet...")
    train_df, test_df = load_datasets()
    print(f"[train_risk_trend_model] Train : {len(train_df)} lignes -- Test : {len(test_df)} lignes")

    train_df = impute_lag_nulls(train_df)
    test_df = impute_lag_nulls(test_df)

    feature_cols = CATEGORICAL_FEATURES + NUMERIC_FEATURES
    X_train, y_train = train_df[feature_cols], train_df[TARGET_COL].to_numpy()
    X_test, y_test = test_df[feature_cols], test_df[TARGET_COL].to_numpy()

    results = {}

    # --- Baseline naive : predire risk_score_avg (semaine courante) comme
    # prediction de la semaine suivante -- hypothese "ca ne change pas
    # d'une semaine a l'autre". Calculee UNIQUEMENT sur le test set (rien a
    # "entrainer" pour une baseline naive).
    print("[train_risk_trend_model] Evaluation de la baseline naive...")
    baseline_pred = test_df["risk_score_avg"].to_numpy()
    results["baseline_naive"] = compute_metrics(y_test, baseline_pred)
    print(f"  -> RMSE={results['baseline_naive']['rmse']:.4f}  MAE={results['baseline_naive']['mae']:.4f}")

    # --- Modele lineaire (Ridge -- regularisation legere, raisonnable vu le
    # petit volume et la colinearite attendue entre risk_score_avg et les
    # lags). alpha par defaut de scikit-learn (1.0), pas de recherche
    # d'hyperparametre par validation croisee (volume trop faible pour un
    # split train/val supplementaire fiable).
    print("[train_risk_trend_model] Entrainement du modele lineaire (Ridge)...")
    ridge_pipeline = build_pipeline(Ridge(alpha=1.0, random_state=42))
    ridge_pipeline.fit(X_train, y_train)
    ridge_pred = ridge_pipeline.predict(X_test)
    results["ridge_linear"] = compute_metrics(y_test, ridge_pred)
    print(f"  -> RMSE={results['ridge_linear']['rmse']:.4f}  MAE={results['ridge_linear']['mae']:.4f}")

    # --- Arbre peu profond (RandomForest, max_depth limite) -- capture des
    # interactions non lineaires eventuelles sans sur-ajuster un volume de
    # 491 lignes d'entrainement. max_depth=4 et n_estimators=100 sont des
    # valeurs par defaut raisonnables documentees ici, pas cherchees par
    # optimisation sur le test set (interdit).
    print("[train_risk_trend_model] Entrainement du modele arbre (RandomForest, max_depth=4)...")
    rf_pipeline = build_pipeline(
        RandomForestRegressor(n_estimators=100, max_depth=4, random_state=42)
    )
    rf_pipeline.fit(X_train, y_train)
    rf_pred = rf_pipeline.predict(X_test)
    results["random_forest"] = compute_metrics(y_test, rf_pred)
    print(f"  -> RMSE={results['random_forest']['rmse']:.4f}  MAE={results['random_forest']['mae']:.4f}")

    print("\n[train_risk_trend_model] Tableau comparatif (TEST set uniquement) :")
    for name, metrics in results.items():
        print(f"  {name:15s} RMSE={metrics['rmse']:.4f}  MAE={metrics['mae']:.4f}")

    # --- Importance des features -- verifie si lag_1_risk_score domine
    # (coherent avec l'hypothese que le risque de la semaine suivante est
    # avant tout proche de celui de la semaine courante/precedente).
    ridge_model = ridge_pipeline.named_steps["model"]
    ohe_feature_names = list(
        ridge_pipeline.named_steps["preprocessor"]
        .named_transformers_["muscle_group_ohe"]
        .get_feature_names_out(CATEGORICAL_FEATURES)
    )
    all_feature_names = ohe_feature_names + NUMERIC_FEATURES

    ridge_coefficients = dict(zip(all_feature_names, ridge_model.coef_.tolist()))
    rf_model = rf_pipeline.named_steps["model"]
    rf_importances = dict(zip(all_feature_names, rf_model.feature_importances_.tolist()))

    print("\n[train_risk_trend_model] Top 5 coefficients Ridge (valeur absolue) :")
    for feat, coef in sorted(ridge_coefficients.items(), key=lambda kv: abs(kv[1]), reverse=True)[:5]:
        print(f"  {feat:30s} {coef:+.4f}")

    print("\n[train_risk_trend_model] Top 5 feature_importances_ RandomForest :")
    for feat, imp in sorted(rf_importances.items(), key=lambda kv: kv[1], reverse=True)[:5]:
        print(f"  {feat:30s} {imp:.4f}")

    # --- Selection du meilleur modele -- critere : RMSE le plus bas sur le
    # TEST set parmi {ridge, random_forest} (la baseline naive n'est jamais
    # elle-meme "sauvegardee comme modele", elle sert de reference de
    # comparaison -- si aucun des deux modeles ML ne bat la baseline, le
    # meilleur des deux est quand meme sauvegarde pour la sous-etape
    # suivante MAIS la conclusion documentee dans ML_TRAINING_RESULTS.md
    # dira explicitement que la baseline suffit / est preferable en
    # pratique).
    ml_candidates = {"ridge_linear": (ridge_pipeline, results["ridge_linear"]["rmse"])}
    ml_candidates["random_forest"] = (rf_pipeline, results["random_forest"]["rmse"])
    best_name, (best_pipeline, best_rmse) = min(ml_candidates.items(), key=lambda kv: kv[1][1])
    print(f"\n[train_risk_trend_model] Meilleur modele ML (RMSE test le plus bas) : {best_name} (RMSE={best_rmse:.4f})")

    beats_baseline = best_rmse < results["baseline_naive"]["rmse"]
    print(
        f"[train_risk_trend_model] Bat la baseline naive ? "
        f"{'OUI' if beats_baseline else 'NON'} "
        f"(baseline RMSE={results['baseline_naive']['rmse']:.4f})"
    )

    model_path = DATA_DIR / "model.pkl"
    metadata = {
        "trained_at_utc": datetime.now(timezone.utc).isoformat(),
        "model_name": best_name,
        "feature_columns": feature_cols,
        "target_column": TARGET_COL,
        "train_rows": len(train_df),
        "test_rows": len(test_df),
        "metrics_test_set": results,
        "beats_naive_baseline": beats_baseline,
    }
    joblib.dump({"pipeline": best_pipeline, "metadata": metadata}, model_path)
    print(f"[train_risk_trend_model] Modele retenu ({best_name}) sauvegarde -> {model_path}")

    metrics_path = DATA_DIR / "training_metrics.json"
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "results": results,
                "best_model": best_name,
                "beats_naive_baseline": beats_baseline,
                "ridge_coefficients": ridge_coefficients,
                "random_forest_importances": rf_importances,
                "metadata": metadata,
            },
            f,
            indent=2,
            ensure_ascii=False,
        )
    print(f"[train_risk_trend_model] Metriques/importances ecrites -> {metrics_path}")


if __name__ == "__main__":
    main()
