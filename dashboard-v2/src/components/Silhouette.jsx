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
// Geometrie INCHANGEE (memes coordonnees exactes que la version
// precedente / que l'ancien dashboard vanilla, viewBox 240x480) : seule la
// source des donnees a change, pas le rendu SVG.

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
  {
    muscle: "shoulder",
    type: "path",
    d: "M107,58 C100,62 94,68 92,78 C97,76 104,71 111,65 C110,62 109,60 107,58 Z",
  },
  {
    muscle: "shoulder",
    type: "path",
    d: "M133,58 C140,62 146,68 148,78 C143,76 136,71 129,65 C130,62 131,60 133,58 Z",
  },
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
    type: "path",
    d: "M150,352 C148,380 148,415 152,445 C156,449 164,449 168,445 C171,415 170,380 166,352 C161,349 155,349 150,352 Z",
  },
];

// Points d'articulation "capture de mouvement" -- OPTIONNEL demande par
// la tache pour renforcer l'effet wireframe technique, non branche sur
// les donnees (purement decoratif, coordonnees approximatives des
// epaules/coudes/hanches/genoux deduites de la geometrie ci-dessus).
const JOINTS = [
  { x: 76, y: 90 }, // epaule gauche
  { x: 164, y: 90 }, // epaule droite
  { x: 44, y: 150 }, // coude gauche
  { x: 196, y: 150 }, // coude droit
  { x: 94, y: 204 }, // hanche gauche
  { x: 146, y: 204 }, // hanche droite
  { x: 98, y: 330 }, // genou gauche
  { x: 142, y: 330 }, // genou droit
];

function zoneColor(muscle, byMuscle) {
  const entry = byMuscle[muscle];
  if (!entry) return COLOR_NONE;
  return COLOR_BY_LEVEL[entry.risk_level] || COLOR_NONE;
}

// Rendu d'une zone en WIREFRAME : fill:none + stroke colore par le risque,
// AUCUNE surface remplie. La lueur (drop-shadow) est appliquee au TRAIT
// lui-meme (deux couches, un halo serre + un halo large, pour un effet
// "trait neon" plutot qu'un nuage flou autour d'une masse) -- UNIQUEMENT
// si une vraie donnee existe pour cette zone (meme regle deja etablie :
// jamais de lueur sur le gris "pas de donnee").
function renderZoneShape(zone, index, byMuscle) {
  const color = zoneColor(zone.muscle, byMuscle);
  const hasData = Boolean(byMuscle[zone.muscle]);
  const commonProps = {
    key: index,
    fill: "none",
    stroke: color,
    strokeWidth: zone.outline ? 1.2 : 1.8,
    strokeDasharray: zone.outline ? "3 2" : undefined,
    style: {
      filter: hasData
        ? `drop-shadow(0 0 2px ${color}) drop-shadow(0 0 7px ${color})`
        : `drop-shadow(0 0 2px ${color})`,
      transition: "stroke 0.3s ease, filter 0.3s ease",
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
  const { muscles, musclesStatus: status, selectedUserId, isDemoMode } = useDashboard();

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

        {/* Zones musculaires (donnees reelles) : wireframe colore par le risque */}
        {ZONES.map((zone, i) => renderZoneShape(zone, i, byMuscle))}

        {/* Sangle abdominale : lignes structurelles */}
        <line x1="120" y1="148" x2="120" y2="192" stroke={STRUCTURE_STROKE} strokeWidth="1" />
        <line x1="102" y1="160" x2="138" y2="160" stroke={STRUCTURE_STROKE} strokeWidth="1" />
        <line x1="102" y1="178" x2="138" y2="178" stroke={STRUCTURE_STROKE} strokeWidth="1" />

        {/* Points d'articulation "motion capture" -- optionnel, purement
            decoratif (non branche aux donnees), renforce l'effet
            wireframe technique. */}
        {JOINTS.map((j, i) => (
          <circle
            key={`joint-${i}`}
            cx={j.x}
            cy={j.y}
            r="2.2"
            fill="var(--color-cyan)"
            style={{ filter: "drop-shadow(0 0 4px var(--color-cyan))" }}
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
    </div>
  );
}
