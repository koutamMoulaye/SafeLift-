// Langage "gros chiffre + label discret", coherent sur tout le dashboard
// (score global, TDEE, proteines, occupation salle...). Chiffre et unite
// TOUJOURS separes en 2 tailles de police distinctes (jamais une seule
// chaine combinee) -- une chaine du type "2446 kcal/jour" a taille "hero"
// deborderait une card etroite (meme correctif deja applique sur l'ancien
// dashboard, voir CLAUDE.md "passe de style holographique").
export default function HeroStat({ value, unit, color }) {
  return (
    <p
      className="truncate text-3xl font-bold leading-tight"
      style={color ? { color, textShadow: `0 0 14px ${color}` } : undefined}
    >
      {value}
      {unit && <span className="ml-1 text-sm font-normal text-slate-400">{unit}</span>}
    </p>
  );
}
