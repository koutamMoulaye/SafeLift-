import { motion } from "framer-motion";
import { useDashboard } from "../hooks/useDashboard";

// SafeLift dashboard-v2 -- silhouette centrale, rendu WIREFRAME.
//
// ⚠️ CORRECTIF CIBLE (2026-07-11) : la silhouette restait figee sur
// user_id=9 (HARDCODED_USER_ID + appel direct a useUserRisk) alors que
// TOUS les autres widgets (TopBar, Zones sensibles, Tendance...) lisent
// deja `muscles`/`musclesStatus`/`selectedUserId` depuis DashboardContext
// -- ce n'etait pas un bug de rendu (les couleurs s'appliquaient bien),
// mais un etat jamais branche sur la meme source de verite que le reste
// du dashboard. Corrige en remplacant l'appel direct a useUserRisk par
// useDashboard() : la silhouette suit desormais le selecteur d'utilisateur
// ET le toggle mode demo EXACTEMENT comme les autres widgets (meme
// `muscles`/`musclesStatus`, meme declencheur).
//
// ⚠️ INTERACTION CLIC + CORRECTIFS D'ALIGNEMENT (2026-07-11) : geometrie
// des zones INCHANGEE (memes coordonnees que l'ancien dashboard vanilla,
// viewBox 240x480), MAIS deux defauts visuels identifies sur l'ancien
// dashboard (et herites tels quels ici, memes coordonnees) sont corriges
// plutot que reproduits -- voir CLAUDE.md pour le detail complet :
//   1. Le glow des trapezes (zones "shoulder" pres du cou) touchait/
//      debordait visuellement sur la tete -- confirme par capture d'ecran
//      zoomee (1px d'ecart seulement entre le sommet du trapeze, y=58, et
//      le bas de l'ellipse de tete, y=57). Corrige en decalant les 2 paths
//      trapezes de +8 en Y (voir TRAPEZIUS_LEFT_D/TRAPEZIUS_RIGHT_D
//      ci-dessous) -- degage un espace reel (~9px) au lieu de quasi-zero.
//   2. Les marqueurs d'articulation "coude"/"hanche" (JOINTS) etaient
//      positionnes sur ou hors du tracé du membre au lieu d'etre centres
//      -- confirme PROGRAMMATIQUEMENT (pas a l'oeil) via
//      SVGGeometryElement.isPointInFill sur les paths bras/jambes : le
//      coude gauche (44,150) tombait exactement sur le bord exterieur du
//      bras (bord gauche du tracé a cette hauteur = x:44, centre reel =
//      x:59.25) ; la hanche gauche (94,204) tombait carrement HORS du
//      tracé des jambes (qui ne commence qu'a y=206). Coordonnees
//      recalculees ci-dessous pour retomber au centre reel du tracé.
// Ajout du clic sur zone (nouveau panneau "Detail de la zone
// selectionnee", voir ZoneDetailPanel.jsx) : geometrie/couleurs des
// zones non affectees, seul un gestionnaire onClick + une couche de
// surbrillance conditionnelle sont ajoutes.

// Trapezes decales de +8 en Y par rapport aux coordonnees d'origine
// (M107,58 ... / M133,58 ...) pour degager la tete -- seule modification
// geometrique de cette passe, documentee ici (pas une regression
// silencieuse : ancien dashboard NON touche, garde ses coordonnees
// d'origine avec le defaut connu).
const TRAPEZIUS_LEFT_D = "M107,66 C100,70 94,76 92,86 C97,84 104,79 111,73 C110,70 109,68 107,66 Z";
const TRAPEZIUS_RIGHT_D = "M133,66 C140,70 146,76 148,86 C143,84 136,79 129,73 C130,70 131,68 133,66 Z";

const COLOR_BY_LEVEL = {
  Faible: "var(--color-risk-faible)",
  Modere: "var(--color-risk-modere)",
  Eleve: "var(--color-risk-eleve)",
};
const COLOR_NONE = "var(--color-risk-none)";

// Couleur neutre des lignes STRUCTURELLES (contour du corps, tete,
// clavicules, ligne mediane) -- volontairement discrete et SANS forte
// lueur, pour que seules les zones a DONNEES REELLES (colorees et
// lumineuses) attirent l'oeil. Hierarchie visuelle : armature technique
// discrete + zones de donnees qui "pulsent".
const STRUCTURE_STROKE = "#3d4f6b";

// Geometrie IDENTIQUE a la version precedente (memes coordonnees, memes
// formes) -- seule la representation change (fill -> stroke).
const ZONES = [
  { muscle: "back", type: "circle", cx: 92, cy: 76, r: 9, outline: true },
  { muscle: "back", type: "circle", cx: 148, cy: 76, r: 9, outline: true },
  { muscle: "shoulder", type: "path", d: TRAPEZIUS_LEFT_D },
  { muscle: "shoulder", type: "path", d: TRAPEZIUS_RIGHT_D },
  { muscle: "shoulder", type: "ellipse", cx: 76, cy: 90, rx: 24, ry: 19 },
  { muscle: "shoulder", type: "ellipse", cx: 164, cy: 90, rx: 24, ry: 19 },
  {
    muscle: "arms",
    type: "path",
    d: "M62,86 C46,102 40,150 46,196 C48,208 52,218 58,224 C64,220 68,208 70,196 C76,150 78,110 74,90 C70,86 66,84 62,86 Z",
  },
  {
    muscle: "arms",
    type: "path",
    d: "M178,86 C194,102 200,150 194,196 C192,208 188,218 182,224 C176,220 172,208 170,196 C164,150 162,110 166,90 C170,86 174,84 178,86 Z",
  },
  { muscle: "chest", type: "ellipse", cx: 103, cy: 110, rx: 19, ry: 24 },
  { muscle: "chest", type: "ellipse", cx: 137, cy: 110, rx: 19, ry: 24 },
  { muscle: "abs", type: "ellipse", cx: 120, cy: 170, rx: 32, ry: 26 },
  { muscle: "lower_back", type: "rect", x: 100, y: 190, width: 40, height: 16, rx: 8, outline: true },
  {
    muscle: "legs",
    type: "path",
    d: "M96,206 C86,240 84,300 88,360 C89,390 91,415 96,432 C100,436 106,436 110,432 C113,415 112,390 110,360 C113,300 116,250 118,210 C112,206 102,205 96,206 Z",
  },
  {
    muscle: "legs",
    type: "path",
    d: "M144,206 C154,240 156,300 152,360 C151,390 149,415 144,432 C140,436 134,436 130,432 C127,415 128,390 130,360 C127,300 124,250 122,210 C128,206 138,205 144,206 Z",
  },
  { muscle: "knee", type: "circle", cx: 98, cy: 330, r: 19 },
  { muscle: "knee", type: "circle", cx: 142, cy: 330, r: 19 },
  {
    muscle: "calves",
    type: "path",
    d: "M90,352 C88,380 88,415 92,445 C96,449 104,449 108,445 C111,415 110,380 106,352 C101,349 95,349 90,352 Z",
  },
  {
    muscle: "calves",
    // ⚠️ BUG REEL trouve et corrige (2026-07-11, signale par l'utilisateur
    // -- "decalage au niveau des tibias") : ce path n'etait PAS le vrai
    // miroir du mollet gauche. Verifie precisement en mirorant chaque
    // point de controle du mollet gauche (x -> 240-x) : le sommet attendu
    // du mollet droit est x:134-150 (centre 142), l'ancien path utilisait
    // x:150-166 (centre 158) -- decale de 16px vers la droite par rapport
    // au vrai miroir, d'ou le mollet visuellement detache/desaligne sous
    // le genou droit. Corrige en recalculant le miroir exact du mollet
    // gauche point par point (plus aucune coordonnee approximee a la main).
    type: "path",
    d: "M150,352 C152,380 152,415 148,445 C144,449 136,449 132,445 C129,415 130,380 134,352 C139,349 145,349 150,352 Z",
  },
];

// Points d'articulation "capture de mouvement" -- OPTIONNEL demande par
// la tache pour renforcer l'effet wireframe technique, non branche sur
// les donnees (purement decoratif, coordonnees approximatives des
// epaules/coudes/hanches/genoux deduites de la geometrie ci-dessus).
const JOINTS = [
  { x: 76, y: 90 }, // epaule gauche -- deja au centre exact de l'ellipse deltoide (cx=76,cy=90), inchange
  { x: 164, y: 90 }, // epaule droite -- idem (cx=164,cy=90), inchange
  { x: 59, y: 150 }, // coude gauche -- recalcule (etait 44, sur le bord exterieur du bras ; centre reel du tracé a y=150 = 59.25, verifie via isPointInFill)
  { x: 181, y: 150 }, // coude droit -- symetrique (240-59)
  { x: 106, y: 210 }, // hanche gauche -- recalcule (etait 94,204, hors du tracé des jambes qui ne commence qu'a y=206 ; centre reel du tracé a y=210 = 106.5)
  { x: 134, y: 210 }, // hanche droite -- symetrique (240-106)
  { x: 98, y: 330 }, // genou gauche -- deja au centre exact du cercle (cx=98,cy=330), inchange
  { x: 142, y: 330 }, // genou droit -- idem (cx=142,cy=330), inchange
];

function zoneColor(muscle, byMuscle) {
  const entry = byMuscle[muscle];
  if (!entry) return COLOR_NONE;
  return COLOR_BY_LEVEL[entry.risk_level] || COLOR_NONE;
}

// Rendu d'une zone EN SURFACE REMPLIE (2026-07-11, demande explicite --
// avant cette passe, seul le TRAIT etait colore, fill:none partout).
// Remplissage translucide (fill-opacity, PAS opaque a 100%) + trait/glow
// conserves pour la lecture de la limite de la zone et l'effet
// holographique deja etabli -- un remplissage opaque plat aurait
// reproduit le look "app tracker" de l'ancien dashboard, explicitement
// evite lors de la refonte wireframe initiale ; le compromis retenu ici
// (surface remplie mais translucide) satisfait la demande ("remplir les
// membres de couleurs") sans revenir a des capsules pleines opaques.
// Cliquable (curseur + onClick) sur toutes les zones -- y compris
// "calves", qui n'a jamais de donnee reelle mais reste cliquable pour
// l'expliquer (voir ZoneDetailPanel.jsx), meme logique que l'ancien
// dashboard.
function renderZoneShape(zone, index, byMuscle, { isSelected, onClick } = {}) {
  const color = zoneColor(zone.muscle, byMuscle);
  const hasData = Boolean(byMuscle[zone.muscle]);
  const commonProps = {
    key: index,
    fill: color,
    fillOpacity: hasData ? 0.38 : 0.14,
    stroke: color,
    strokeWidth: (zone.outline ? 1.2 : 1.6) + (isSelected ? 0.6 : 0),
    strokeDasharray: zone.outline ? "3 2" : undefined,
    className: "cursor-pointer",
    onClick: () => onClick?.(zone.muscle),
    style: {
      // Note : avec un fill reel (plus fill:none), pointer-events:fill
      // n'est plus strictement necessaire (le fill peint repond deja aux
      // clics par defaut) -- conserve explicitement pour rester robuste
      // si fillOpacity venait a redescendre pres de 0 pour une raison
      // future (ex. zone "pas de donnee" tres transparente).
      pointerEvents: "fill",
      // Selection : couche de glow blanche additionnelle EN PLUS de la
      // couleur de risque (jamais a la place) -- meme principe que
      // .zone.selected de l'ancien dashboard (brightness + halo blanc),
      // porte en inline style ici (pas de classe CSS globale sur un SVG
      // genere dynamiquement).
      filter: isSelected
        ? `brightness(1.3) drop-shadow(0 0 3px rgba(255,255,255,0.85)) drop-shadow(0 0 8px ${color})`
        : hasData
          ? `drop-shadow(0 0 2px ${color}) drop-shadow(0 0 7px ${color})`
          : `drop-shadow(0 0 2px ${color})`,
      transition: "fill-opacity 0.3s ease, stroke 0.3s ease, filter 0.3s ease, stroke-width 0.3s ease",
    },
  };

  switch (zone.type) {
    case "circle":
      return <circle {...commonProps} cx={zone.cx} cy={zone.cy} r={zone.r} />;
    case "ellipse":
      return <ellipse {...commonProps} cx={zone.cx} cy={zone.cy} rx={zone.rx} ry={zone.ry} />;
    case "rect":
      return (
        <rect {...commonProps} x={zone.x} y={zone.y} width={zone.width} height={zone.height} rx={zone.rx} />
      );
    case "path":
      return <path {...commonProps} d={zone.d} />;
    default:
      return null;
  }
}

export default function Silhouette() {
  const { muscles, musclesStatus: status, selectedUserId, isDemoMode, selectedMuscle, setSelectedMuscle } =
    useDashboard();

  const byMuscle = {};
  muscles.forEach((m) => (byMuscle[m.muscle_group] = m));

  return (
    <div className="relative flex flex-col items-center justify-center">
      {/* Halo d'ambiance derriere la silhouette (purement decoratif, tres
          discret -- ne doit jamais donner l'impression d'une masse
          pleine, juste une teinte de fond) */}
      <div
        className="absolute inset-0 -z-10 blur-3xl opacity-20"
        style={{
          background:
            "radial-gradient(ellipse 260px 380px at 50% 45%, var(--color-cyan), transparent 70%)",
        }}
      />

      {/* Animation de "respiration" tres subtile (scale/opacity en boucle) --
          Framer Motion, demandee explicitement pour l'ambiance holographique. */}
      <motion.svg
        viewBox="0 0 240 480"
        className="w-full max-w-[320px]"
        animate={{ scale: [1, 1.015, 1], opacity: [1, 0.97, 1] }}
        transition={{ duration: 4.5, repeat: Infinity, ease: "easeInOut" }}
      >
        {/* Quadrillage technique en fond (2026-07-11, demande explicite --
            grille "HUD" derriere la silhouette, absente jusqu'ici). Motif
            SVG repete (pas une image), a l'INTERIEUR du meme <svg> que la
            silhouette pour suivre exactement la meme animation/mise a
            l'echelle. Estompe vers les bords via un masque a degrade
            radial (`gridFade`) -- une grille uniforme jusqu'au bord aurait
            attire l'oeil davantage que la silhouette elle-meme, contraire
            a l'esprit "cadre technique discret" deja etabli pour
            l'armature structurelle. */}
        <defs>
          <pattern id="techGrid" width="20" height="20" patternUnits="userSpaceOnUse">
            <path d="M 20 0 L 0 0 0 20" fill="none" stroke="var(--color-cyan)" strokeWidth="0.5" />
          </pattern>
          <radialGradient id="gridFadeGradient" cx="50%" cy="40%" r="65%">
            <stop offset="0%" stopColor="white" stopOpacity="1" />
            <stop offset="100%" stopColor="white" stopOpacity="0" />
          </radialGradient>
          <mask id="gridFadeMask">
            <rect x="0" y="0" width="240" height="480" fill="url(#gridFadeGradient)" />
          </mask>
        </defs>
        <rect
          x="0"
          y="0"
          width="240"
          height="480"
          fill="url(#techGrid)"
          mask="url(#gridFadeMask)"
          opacity="0.4"
        />

        {/* Armature structurelle : tete/cou/contour/clavicules/ligne
            mediane -- TOUT en fill:none + stroke fin, couleur neutre
            discrete (STRUCTURE_STROKE), sans lueur forte (juste une legere
            netteness via drop-shadow tres discret). C'est le "cadre"
            technique du wireframe, pas une donnee. */}
        <ellipse
          cx="120"
          cy="32"
          rx="20"
          ry="25"
          fill="none"
          stroke={STRUCTURE_STROKE}
          strokeWidth="1.3"
        />
        <path
          fill="none"
          stroke={STRUCTURE_STROKE}
          strokeWidth="1.3"
          d="M108,52 C108,58 110,62 112,64 L128,64 C130,62 132,58 132,52 C132,48 126,46 120,46 C114,46 108,48 108,52 Z"
        />

        <path
          fill="none"
          stroke={STRUCTURE_STROKE}
          strokeWidth="1.5"
          style={{ filter: "drop-shadow(0 0 3px rgba(0,229,255,0.25))" }}
          d="M104,58 C95,58 80,66 74,84 C68,108 82,138 92,172 C88,186 84,192 84,202 C84,206 100,208 120,208 C140,208 156,206 156,202 C156,192 152,186 148,172 C158,138 172,108 166,84 C160,66 145,58 136,58 C130,63 110,63 104,58 Z"
        />

        {/* Ligne mediane du torse (colonne/sternum schematises) */}
        <path
          fill="none"
          stroke={STRUCTURE_STROKE}
          strokeWidth="1"
          strokeDasharray="2 3"
          d="M120,64 C119,100 121,140 120,172 C120,185 120,196 120,206"
        />

        <path fill="none" stroke={STRUCTURE_STROKE} strokeWidth="1.2" d="M110,60 C104,62 98,64 94,68" />
        <path fill="none" stroke={STRUCTURE_STROKE} strokeWidth="1.2" d="M130,60 C136,62 142,64 146,68" />

        {/* Zones musculaires (donnees reelles) : wireframe colore par le risque,
            cliquables -- ouvre le panneau "Detail de la zone selectionnee". */}
        {ZONES.map((zone, i) =>
          renderZoneShape(zone, i, byMuscle, {
            isSelected: selectedMuscle === zone.muscle,
            onClick: setSelectedMuscle,
          })
        )}

        {/* Sangle abdominale : lignes structurelles */}
        <line x1="120" y1="148" x2="120" y2="192" stroke={STRUCTURE_STROKE} strokeWidth="1" />
        <line x1="102" y1="160" x2="138" y2="160" stroke={STRUCTURE_STROKE} strokeWidth="1" />
        <line x1="102" y1="178" x2="138" y2="178" stroke={STRUCTURE_STROKE} strokeWidth="1" />

        {/* Points d'articulation "motion capture" -- optionnel, purement
            decoratif (non branche aux donnees), renforce l'effet
            wireframe technique.
            ⚠️ BUG REEL trouve et corrige (2026-07-11, pendant l'ajout du
            clic sur zone) : ces points sont rendus APRES les zones (donc
            au-dessus dans l'empilement SVG) et certains coincident
            EXACTEMENT avec le centre d'une zone cliquable (ex. le point
            du genou gauche, x=98/y=330, tombe pile sur le centre du
            cercle "knee" de meme centre) -- confirme via
            `document.elementFromPoint()` : le petit cercle decoratif
            (fill plein, r=2.2) interceptait le clic destine a la zone
            EN DESSOUS. `pointer-events: none` explicite : purement
            visuel, ne doit jamais faire obstacle a une zone interactive
            sous-jacente. */}
        {JOINTS.map((j, i) => (
          <circle
            key={`joint-${i}`}
            cx={j.x}
            cy={j.y}
            r="2.2"
            fill="var(--color-cyan)"
            style={{ filter: "drop-shadow(0 0 4px var(--color-cyan))", pointerEvents: "none" }}
          />
        ))}
      </motion.svg>

      {/* Statut du chargement -- suit desormais selectedUserId/isDemoMode
          (DashboardContext), plus HARDCODED_USER_ID. */}
      <p className="mt-4 text-xs text-slate-400">
        {status === "loading" && "Chargement des données de risque…"}
        {status === "error" && <span className="text-red-400">Erreur de chargement.</span>}
        {status === "ready" &&
          (isDemoMode
            ? `Scénario démo — ${muscles.length} zone(s)`
            : `Utilisateur ${selectedUserId} — ${muscles.length} zone(s) avec données`)}
      </p>

      {/* Legende des couleurs -- PORTEE depuis l'ancien dashboard
          (dashboard/static/index.html, <ul class="legend">), absente de
          dashboard-v2 jusqu'ici (signale par l'utilisateur, 2026-07-11).
          Memes 4 niveaux/memes seuils, restyle en pastilles avec lueur
          pour rester coherent avec le rendu wireframe/holographique
          (au lieu des carres pleins "swatch" de l'ancien dashboard). */}
      <ul className="mt-3 flex flex-wrap items-center justify-center gap-x-4 gap-y-1.5 text-[11px] text-slate-400">
        {[
          { label: "Faible (0-33)", color: "var(--color-risk-faible)" },
          { label: "Modéré (34-66)", color: "var(--color-risk-modere)" },
          { label: "Élevé (67-100)", color: "var(--color-risk-eleve)" },
          { label: "Pas de donnée", color: "var(--color-risk-none)" },
        ].map((item) => (
          <li key={item.label} className="flex items-center gap-1.5">
            <span
              className="inline-block h-2 w-2 shrink-0 rounded-full"
              style={{ background: item.color, boxShadow: `0 0 5px ${item.color}` }}
            />
            {item.label}
          </li>
        ))}
      </ul>
    </div>
  );
}
