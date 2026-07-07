#!/usr/bin/env bash
# ============================================================
# SafeLift — Téléchargement des datasets Bronze depuis Kaggle
# Compatible avec le nouveau format de token Kaggle (KGAT_...)
#
# Usage (3 options) :
#   Option A — token en variable d'environnement (recommandé) :
#     KAGGLE_API_TOKEN=KGAT_xxx bash data/download_datasets.sh
#
#   Option B — token passé en argument :
#     bash data/download_datasets.sh KGAT_xxx
#
#   Option C — token déjà dans ~/.kaggle/kaggle.json :
#     bash data/download_datasets.sh
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_DIR="$REPO_ROOT/.venv"

# --- Résolution du token ---
TOKEN="${1:-${KAGGLE_API_TOKEN:-}}"

if [ -n "$TOKEN" ]; then
  echo "[INFO] Token KGAT détecté — configuration automatique..."
  mkdir -p ~/.kaggle

  # Récupérer le username depuis l'API Kaggle avec le token
  USERNAME=$(curl -s -H "Authorization: Bearer $TOKEN" \
    "https://www.kaggle.com/api/v1/users/me" | python3 -c \
    "import sys,json; d=json.load(sys.stdin); print(d.get('userName',''))" 2>/dev/null || true)

  if [ -z "$USERNAME" ]; then
    echo "[WARN] Impossible de récupérer le username automatiquement."
    echo "       Entrez votre username Kaggle (visible sur https://www.kaggle.com/settings) :"
    read -r USERNAME
  fi

  # Écrire kaggle.json au format classique accepté par la CLI
  cat > ~/.kaggle/kaggle.json <<EOF
{"username":"${USERNAME}","key":"${TOKEN}"}
EOF
  chmod 600 ~/.kaggle/kaggle.json
  echo "[OK] ~/.kaggle/kaggle.json configuré pour l'utilisateur : $USERNAME"
fi

# --- Environnement Python isolé (venv dédié au projet, à la racine du repo) ---
# Evite les conflits d'installation entre paquets système (dist-packages) et
# installs --user, source d'un bug déjà rencontré : kagglesdk 0.1.31/0.1.32
# contiennent un import cassé dans leur propre wheel PyPI
# (ModuleNotFoundError: No module named 'kagglesdk.competitions.legacy').
# On pin donc les versions dans data/requirements.txt, installées uniquement
# dans ce venv, jamais sur le système.
if [ ! -f "$VENV_DIR/bin/activate" ]; then
  echo "[INFO] Création du venv Python du projet : $VENV_DIR"
  if ! python3 -m venv "$VENV_DIR" 2>/dev/null; then
    # Sur certaines distros (ex: Ubuntu WSL sans le paquet system python3-venv),
    # le module venv standard échoue car ensurepip est absent et nécessite sudo.
    # On bascule alors sur le paquet PyPI "virtualenv", qui embarque son propre
    # pip et ne nécessite aucun paquet système ni privilège root.
    echo "[INFO] python3 -m venv indisponible (ensurepip manquant) — bascule sur le paquet 'virtualenv'"
    pip install --user --break-system-packages -q virtualenv
    export PATH="$PATH:$HOME/.local/bin"
    python3 -m virtualenv -q "$VENV_DIR"
  fi
fi

# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

echo "[INFO] Vérification de la CLI Kaggle dans le venv ($VENV_DIR)..."
pip install -q -r "$SCRIPT_DIR/requirements.txt"

if [ ! -f ~/.kaggle/kaggle.json ]; then
  echo "[ERREUR] Aucun token configuré."
  echo "  Lancez : KAGGLE_API_TOKEN=KGAT_xxx bash data/download_datasets.sh"
  exit 1
fi

# --- Destination ---
DEST_DIR="$SCRIPT_DIR/bronze/raw"
mkdir -p "$DEST_DIR"

# --- Téléchargements ---
echo ""
echo "========================================================"
echo " Dataset 1 : 600K+ Fitness Exercise & Workout Program"
echo "========================================================"
kaggle datasets download \
  -d adnanelouardi/600k-fitness-exercise-and-workout-program-dataset \
  -p "$DEST_DIR/600k_fitness" --unzip
echo "[OK] → $DEST_DIR/600k_fitness/"

echo ""
echo "========================================================"
echo " Dataset 2 : Gym Members Exercise Dataset"
echo "========================================================"
kaggle datasets download \
  -d valakhorasani/gym-members-exercise-dataset \
  -p "$DEST_DIR/gym_members" --unzip
echo "[OK] → $DEST_DIR/gym_members/"

echo ""
echo "========================================================"
echo " Dataset 3 : 721 Weight Training Workouts"
echo "========================================================"
kaggle datasets download \
  -d joep89/weightlifting \
  -p "$DEST_DIR/weight_training" --unzip
echo "[OK] → $DEST_DIR/weight_training/"

# --- Rapport ---
echo ""
echo "========================================================"
echo " Rapport : schéma & taille des fichiers"
echo "========================================================"
DEST_DIR="$DEST_DIR" python3 - <<'PYEOF'
import os, glob, csv

# DEST_DIR est transmis via l'environnement plutôt que dérivé de __file__ :
# sous "python3 - <<EOF", __file__ vaut "<stdin>", ce qui casse la résolution
# de chemin relative au script.
BASE = os.environ["DEST_DIR"]

for csv_path in sorted(glob.glob(f"{BASE}/**/*.csv", recursive=True)):
    with open(csv_path, encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        headers = next(reader, [])
        rows = sum(1 for _ in reader)
    size_mb = os.path.getsize(csv_path) / 1_048_576
    rel = os.path.relpath(csv_path, BASE)
    print(f"\n  {rel}")
    print(f"  Lignes : {rows:,}  |  Colonnes : {len(headers)}  |  {size_mb:.2f} MB")
    print(f"  {', '.join(headers)}")
PYEOF

echo ""
echo "Datasets disponibles dans : $DEST_DIR"
echo "Pense à regénérer ton token Kaggle sur https://www.kaggle.com/settings"
