"""Built-in agent instruction for safe XPDE analysis."""

ANALYZE_CURRENT_PROMPT = """\
Gunakan tool xpde_analyze_current untuk profile {profile}.

Jawab berurutan:
1. Status data dan apakah forecast current.
2. Forecast XPDE H1/H3/H6/H12 beserta q10-q90.
3. P(UP) dan P(NON-UP); NON-UP berarti negatif atau flat, bukan strict DOWN.
4. Technical context M5/M15/H1 dari completed bars.
5. Support, resistance, confluence, conflict, dan limitation.
6. Exact core proposal serta reason codes.
7. Offline evidence, live evidence, dan WARMING_UP/HEALTHY/DEGRADED/SUSPENDED.
8. Kesimpulan sebagai guide, bukan jaminan.

Wajib:
- Pertahankan exact keputusan XPDE core.
- Jangan membuat BUY/SELL alternatif.
- Jangan menyatakan origin probability sebagai current-entry probability.
- WARMING_UP berarti forecast tetap dapat dianalisis, tetapi live evidence belum matang.
- DEGRADED berarti guide hanya boleh dibaca dengan peringatan kuat.
- SUSPENDED berarti forecast diagnostic only, bukan guide.
- WAIT dan NO_PREDICTION adalah output core yang valid.
- Jangan mengeksekusi order, mengirim feedback, mempromosikan model, retraining,
  membuat forecast baru, atau mengubah database/policy.
"""


def analyze_current_prompt(profile: str) -> str:
    value = profile.upper()
    if value not in {"SCALPER", "SNIPER"}:
        value = "SCALPER"
    return ANALYZE_CURRENT_PROMPT.format(profile=value)
