import { useMemo, useState } from "react";
import { useDashboard } from "../hooks/useDashboard";
import { computeGlobalScore, levelColor, truncateForSelect } from "../lib/riskHelpers";

// Barre superieure : selecteur d'utilisateur (profils avec donnees en
// premier, toujours pre-selectionne) + toggle demo + score global reel
// (MAX des risk_score par zone, pas une moyenne -- voir riskHelpers).
// Visible en permanence quel que soit le reste de la page (reglages
// globaux, pas propres a un widget).
//
// Selecteur allege (2026-07-10) : les 972 profils SANS donnee reelle ne
// sont plus enumeres un par un dans le <select> (illisible, penible a
// parcourir) -- remplaces par une seule ligne resumee. Un utilisateur
// SANS donnee reste neanmoins accessible via un champ de recherche par
// ID (utile pour tester le cas "non disponible" sur Nutrition/ML) --
// implemente en `<input list>` + `<datalist>` natif : le filtrage de la
// liste de suggestions au fur et a mesure de la frappe est gere par le
// navigateur lui-meme, pas de logique de filtrage custom a ecrire.
export default function TopBar() {
  const {
    usersWithData,
    usersWithoutData,
    usersStatus,
    selectedUserId,
    setSelectedUserId,
    isDemoMode,
    setIsDemoMode,
    demoScenarios,
    selectedScenarioId,
    setSelectedScenarioId,
    muscles,
    musclesStatus,
  } = useDashboard();

  const [searchOpen, setSearchOpen] = useState(false);
  const [searchValue, setSearchValue] = useState("");

  const today = new Date().toLocaleDateString("fr-FR", {
    weekday: "long",
    day: "numeric",
    month: "long",
  });

  const global = computeGlobalScore(muscles);
  const scoreColor = global ? levelColor(global.risk_level) : "#3a4152";

  // Index de TOUS les utilisateurs (avec + sans donnee), pour valider un
  // ID tape dans la recherche et alimenter le <datalist>.
  const allUsersById = useMemo(() => {
    const map = new Map();
    usersWithData.forEach((u) => map.set(u.user_id, { ...u, hasData: true }));
    usersWithoutData.forEach((u) => map.set(u.user_id, { ...u, hasData: false }));
    return map;
  }, [usersWithData, usersWithoutData]);

  // Si l'utilisateur actuellement selectionne vient du groupe "sans
  // donnee" (choisi via la recherche), le <select> principal ne liste
  // plus ses options individuellement -- une option temporaire est donc
  // injectee pour que le <select> reflete correctement la selection en
  // cours, plutot que d'afficher un champ vide/incoherent.
  const selectedWithoutData = usersWithoutData.find((u) => u.user_id === selectedUserId);

  function trySelectFromSearch() {
    const id = parseInt(searchValue, 10);
    if (!Number.isNaN(id) && allUsersById.has(id)) {
      setSelectedUserId(id);
    }
  }

  const totalUsers = usersWithData.length + usersWithoutData.length;

  return (
    <header className="sticky top-0 z-20 border-b border-line bg-panel/90 px-8 py-4 backdrop-blur">
      {isDemoMode && (
        <div className="mb-3 rounded-lg border border-amber-500/40 bg-amber-500/10 px-3 py-1.5 text-xs font-medium text-amber-300">
          Mode démonstration actif — scénario synthétique affiché, jamais mélangé aux données réelles.
        </div>
      )}
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div className="flex min-w-0 items-center gap-3">
          <span className="h-2 w-2 shrink-0 animate-pulse rounded-full bg-cyan shadow-[0_0_8px_var(--color-cyan)]" />
          <h1 className="shrink-0 text-lg font-bold tracking-wide text-white">
            SafeLift <span className="text-cyan">v2</span>
          </h1>
        </div>

        <div className="flex min-w-0 flex-1 flex-col items-center justify-center gap-1.5">
          {!isDemoMode ? (
            <>
              <div className="flex flex-wrap items-center justify-center gap-2">
                <select
                  className="min-w-0 max-w-[280px] truncate rounded-lg border border-line bg-panel-alt px-3 py-1.5 text-sm text-slate-200"
                  value={selectedUserId ?? ""}
                  onChange={(e) => setSelectedUserId(Number(e.target.value))}
                  title={
                    usersStatus === "loading"
                      ? "Chargement des utilisateurs…"
                      : `Utilisateur ${selectedUserId ?? "—"}`
                  }
                >
                  {usersStatus === "loading" && <option>Chargement…</option>}
                  {usersWithData.length > 0 && (
                    <optgroup label="Profils avec données de séance réelles">
                      {usersWithData.map((u) => (
                        <option key={u.user_id} value={u.user_id}>
                          Utilisateur {u.user_id} ({u.age} ans, {u.gender})
                        </option>
                      ))}
                    </optgroup>
                  )}
                  {/* Option temporaire : uniquement presente si le profil
                      selectionne vient de la recherche (pas enumere ci-dessus). */}
                  {selectedWithoutData && (
                    <option value={selectedWithoutData.user_id}>
                      Utilisateur {selectedWithoutData.user_id} — pas de séance (recherché)
                    </option>
                  )}
                </select>

                {usersWithoutData.length > 0 && (
                  <button
                    type="button"
                    onClick={() => setSearchOpen((v) => !v)}
                    className="shrink-0 rounded-lg border border-dashed border-line px-2 py-1.5 text-xs text-slate-500 transition hover:border-slate-500 hover:text-slate-300"
                  >
                    +{usersWithoutData.length} profils sans séance réelle (démonstration) — rechercher un ID
                    {searchOpen ? " ▲" : " ▼"}
                  </button>
                )}
              </div>

              {searchOpen && (
                <div className="flex items-center gap-2">
                  <input
                    type="text"
                    inputMode="numeric"
                    list="all-users-datalist"
                    value={searchValue}
                    onChange={(e) => setSearchValue(e.target.value)}
                    onKeyDown={(e) => e.key === "Enter" && trySelectFromSearch()}
                    onBlur={trySelectFromSearch}
                    placeholder="Numéro d'utilisateur (ex: 42)"
                    className="w-56 rounded-lg border border-line bg-panel-alt px-2 py-1 text-xs text-slate-200"
                  />
                  {/* Filtrage des suggestions gere nativement par le
                      navigateur au fur et a mesure de la frappe -- pas de
                      logique de filtrage custom. */}
                  <datalist id="all-users-datalist">
                    {Array.from(allUsersById.values()).map((u) => (
                      <option key={u.user_id} value={u.user_id}>
                        Utilisateur {u.user_id} {u.hasData ? "(données réelles)" : "— pas de séance"}
                      </option>
                    ))}
                  </datalist>
                  {searchValue && !allUsersById.has(parseInt(searchValue, 10)) && (
                    <span className="text-[11px] text-red-400">ID inconnu</span>
                  )}
                </div>
              )}

              <p className="text-center text-[11px] text-slate-500">
                {totalUsers} profil{totalUsers > 1 ? "s" : ""} au total — {usersWithData.length} avec donnée
                {usersWithData.length > 1 ? "s" : ""} réelle{usersWithData.length > 1 ? "s" : ""}
                {selectedUserId &&
                  (global
                    ? ` — profil sélectionné : score ${global.risk_score.toFixed(0)} (${global.risk_level})`
                    : musclesStatus === "ready"
                      ? " — profil sélectionné : aucune donnée de risque"
                      : "")}
              </p>
            </>
          ) : (
            <select
              className="min-w-0 max-w-[320px] truncate rounded-lg border border-line bg-panel-alt px-3 py-1.5 text-sm text-slate-200"
              value={selectedScenarioId ?? ""}
              onChange={(e) => setSelectedScenarioId(Number(e.target.value))}
            >
              {demoScenarios.length === 0 && <option>Chargement des scénarios…</option>}
              {demoScenarios.map((s) => {
                const full = `#${s.scenario_id} — ${s.scenario_label} (${s.risk_level})`;
                return (
                  <option key={s.scenario_id} value={s.scenario_id} title={full}>
                    {truncateForSelect(full)}
                  </option>
                );
              })}
            </select>
          )}

          <label className="flex shrink-0 cursor-pointer items-center gap-2 text-xs text-slate-400">
            <input
              type="checkbox"
              checked={isDemoMode}
              onChange={(e) => setIsDemoMode(e.target.checked)}
              className="h-4 w-4 accent-violet"
            />
            Mode démo
          </label>
        </div>

        <div className="flex shrink-0 items-center gap-4">
          <div className="text-right text-sm text-slate-400">
            <div className="text-[10px] uppercase tracking-wide text-slate-500">Score global</div>
            {musclesStatus === "loading" ? (
              <span className="text-slate-600">…</span>
            ) : global ? (
              <span
                className="text-xl font-bold"
                style={{ color: scoreColor, textShadow: `0 0 10px ${scoreColor}` }}
              >
                {global.risk_score.toFixed(0)} <span className="text-xs font-normal">({global.risk_level})</span>
              </span>
            ) : (
              <span className="text-slate-600">— (aucune donnée)</span>
            )}
          </div>
          <div className="hidden text-sm capitalize text-slate-400 sm:block">{today}</div>
        </div>
      </div>
    </header>
  );
}
