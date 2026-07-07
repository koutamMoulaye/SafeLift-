"""SafeLift — Etape 6/6, sous-etape 4/6 : pseudonymisation HMAC-SHA256 des identifiants utilisateur.

Contexte / decision d'architecture (voir CLAUDE.md pour le detail complet) :
le pipeline INTERNE (Bronze -> Silver -> Gold Postgres, API du dashboard de
demo) conserve `user_id` en clair : c'est un environnement controle (reseau
Docker interne / localhost, jamais expose sur Internet), et `user_id` y sert
de cle de jointure technique pour tout le modele en etoile (facts <-> dim_user).
Re-architecturer l'integralite de l'API/dashboard autour d'un identifiant
pseudonymise casserait cette simplicite pour un gain de securite nul dans un
contexte 100% local/pedagogique.

La pseudonymisation s'applique a la COUCHE DE RESTITUTION EXTERNE : l'export
S3/Athena (scripts/upload_gold_to_s3.py), qui est le point ou les donnees
quittent l'environnement controle pour un compte AWS cloud, potentiellement
interrogeable par des outils/tiers plus larges. Cette couche remplace
`user_id` par `user_pseudo_id` (voir upload_gold_to_s3.py et terraform/athena.tf).

Pourquoi HMAC-SHA256 calcule ici en Python et PAS dans un modele dbt (malgre
la suggestion initiale de "modele dbt dedie ou vue") : dbt substitue les
valeurs de `env_var()` EN CLAIR dans le SQL compile (dbt/target/compiled/...,
relisible sur disque) et donc potentiellement dans les logs de requetes
Postgres -- ce qui va a l'encontre meme de l'objectif de ne jamais exposer la
cle secrete. Le calcul en Python pur (ce module, aucun acces DB) evite que la
cle transite par une couche SQL/logs.

Deux user_id distincts ne DOIVENT jamais produire le meme pseudonyme (verifie
par test), et sans la cle il est calculatoirement impossible de retrouver
user_id a partir de user_pseudo_id (propriete d'irreversibilite standard de
HMAC).
"""

import hashlib
import hmac
import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_VAR_NAME = "PSEUDONYMIZATION_KEY"


def _load_env_file(path: Path) -> dict[str, str]:
    """Parseur minimal de fichier .env (KEY=VALUE), sans dependance externe.

    Duplique volontairement de la fonction identique dans upload_gold_to_s3.py
    (module autonome, pas de dependance croisee entre scripts utilitaires).
    """
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip()
    return values


def load_pseudonymization_key() -> str:
    """Lit PSEUDONYMIZATION_KEY depuis l'environnement (ou .env en repli local).

    Leve une erreur explicite plutot que de se rabattre silencieusement sur une
    cle par defaut : une pseudonymisation avec une cle connue/devinable n'a
    aucune valeur de protection.
    """
    key = os.environ.get(ENV_VAR_NAME)
    if not key:
        key = _load_env_file(REPO_ROOT / ".env").get(ENV_VAR_NAME)
    if not key:
        raise RuntimeError(
            f"{ENV_VAR_NAME} n'est pas definie (ni dans l'environnement, ni dans .env). "
            "Voir .env.example pour le format attendu -- generer une cle forte "
            "avec, par exemple, `python -c \"import secrets; print(secrets.token_hex(32))\"`."
        )
    return key


def pseudonymize_user_id(user_id: int, key: str) -> str:
    """HMAC-SHA256(user_id) -> pseudonyme hexadecimal stable (64 caracteres).

    Meme user_id + meme cle => toujours le meme pseudonyme (necessaire pour
    que les jointures fact_* <-> dim_user restent valides une fois
    pseudonymisees sur la couche de restitution externe). Cle differente ou
    absente => impossible de retrouver la correspondance.
    """
    return hmac.new(
        key.encode("utf-8"),
        str(user_id).encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


if __name__ == "__main__":
    # Auto-test rapide, execute directement (`python scripts/pseudonymize.py`) :
    # verifie la coherence (meme id -> meme pseudo) et la sensibilite a la cle
    # (cle differente -> pseudo different), sans dependance a pytest.
    key_a = "clé-de-test-A"
    key_b = "clé-de-test-B"

    p9_first = pseudonymize_user_id(9, key_a)
    p9_second = pseudonymize_user_id(9, key_a)
    p4 = pseudonymize_user_id(4, key_a)
    p9_other_key = pseudonymize_user_id(9, key_b)

    assert p9_first == p9_second, "meme user_id + meme cle doit produire le meme pseudonyme"
    assert p9_first != p4, "deux user_id distincts ne doivent jamais produire le meme pseudonyme"
    assert p9_first != p9_other_key, "une cle differente doit produire un pseudonyme different"
    assert len(p9_first) == 64, "HMAC-SHA256 en hexadecimal doit faire 64 caracteres"

    print("[pseudonymize] Tous les tests de coherence sont passes.")
    print(f"  user_id=9  (cle A) -> {p9_first}")
    print(f"  user_id=4  (cle A) -> {p4}")
    print(f"  user_id=9  (cle B) -> {p9_other_key}")
