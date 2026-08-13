"""
BBN V2 inference for the self-review case study (SQ1.1) -- per-gap plate model.

The latent node is G = is_genuine in {genuine, not_genuine} (presumption of
innocence: the named state is innocence; we escalate cases whose genuineness is
implausibly low). Reads bbn_baselines/bbn_v2_baseline_<journal>.json (from
bbn_extract_v2.py) and, for each investigated author, multiplies the per-gap
likelihood ratios:

    posterior_odds(genuine) = prior_odds(genuine) * PRODUCT_over_gaps  LR(gap_i)

    LR(gap) = P(b | genuine, journal, type, pages) / P(b | not_genuine, journal, type, pages)
            = genuine_b / ( alpha * P(b | manipulated) + (1 - alpha) * genuine_b )

where b is the gap's z-bin. The genuine term genuine_b is the JOURNAL-SPECIFIC
empirical baseline -- a genuine author's gaps follow the peer distribution (each
gap judged by its own journal). The not_genuine branch is the alpha-mixture: a
fraction alpha of an author's papers are manipulated (-> near-instant review,
MANIP_DIST), the rest behave genuinely. An incriminating (fast) gap therefore
yields LR < 1 and *lowers* genuineness.

Only alpha and MANIP_DIST are elicited; the type/pages influence is entirely in
the empirical genuine baseline.

Genuine-baseline back-off ladder (so no hard zeros, and pages=unknown is treated
as missing rather than a category):
    journal type+pages  ->  journal type (pages marginal)  ->  pooled type
all Laplace-smoothed.

Output is P(is_genuine | evidence); investigation priority = lowest genuineness
first. This is a ranking to support manual review, never an accusation.

Run from this directory (BBN/) with the project venv:
    python bbn_infer_v2.py
"""

from __future__ import annotations

import json
import os
from collections import defaultdict

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

JOURNAL_ID = "10623"
IN_PATH = os.path.join(os.path.dirname(__file__), "bbn_baselines", f"bbn_v2_baseline_{JOURNAL_ID}.json")

# Prior probability that an author is genuine (high, by presumption of innocence).
PRIOR_GENUINE = 0.95
PRIOR_GENUINE_SWEEP = [0.99, 0.98, 0.95, 0.90, 0.80]

ALPHA = 0.10                              # fraction of a non-genuine author's papers that are manipulated
ALPHA_SWEEP = [0.02, 0.05, 0.10, 0.25, 0.50]

# What a manipulated paper's gap looks like (the not_genuine component). Most mass
# on the extreme bins (self-review is usually a big speedup); mild_fast is armed
# above the ~0.13 genuine rate so a chronically slightly-fast author still loses
# genuineness (at the cost of specificity vs an efficient genuine author).
# Keys must match z_bins; sums to 1.
MANIP_DIST = {"typical": 0.0, "mild_fast": 0.25, "extreme": 0.25, "very_extreme": 0.50}

LAPLACE = 0.5
MIN_STRATUM = 30                          # need this many papers to trust a journal stratum


def confidence_band(n_gaps):
    """How much record we have to contextualize the verdict (NOT strength of guilt)."""
    if n_gaps <= 2:  return "low"
    if n_gaps <= 9:  return "medium"
    return "high"


# ---------------------------------------------------------------------------
# Genuine baseline with back-off  (= P(gap | genuine, journal, type, pages))
# ---------------------------------------------------------------------------

def smooth(counts, bins):
    total = sum(counts.get(b, 0) for b in bins) + LAPLACE * len(bins)
    return {b: (counts.get(b, 0) + LAPLACE) / total for b in bins}


def build_pooled(journals, bins):
    """Pooled (all-journal) type-marginal counts, for the last back-off level."""
    pooled = defaultdict(lambda: defaultdict(int))
    for j in journals.values():
        for sk, binc in j["baseline_counts"].items():
            tbin = sk.split("|")[0]
            for b in bins:
                pooled[tbin][b] += binc.get(b, 0)
    return pooled


def genuine_dist(journals, pooled, bins, jid, tbin, pbin):
    """Return (smoothed P(bin | genuine, journal, type, pages), level-label) via back-off."""
    jbase = journals.get(jid, {}).get("baseline_counts", {})

    # level 1: journal-specific type+pages (only if pages known)
    if pbin not in ("unknown", None):
        c = jbase.get(f"{tbin}|{pbin}")
        if c and sum(c.values()) >= MIN_STRATUM:
            return smooth(c, bins), "journal:type+pages"

    # level 2: journal type marginal (sum over pages)
    ctm = defaultdict(int)
    for sk, binc in jbase.items():
        if sk.split("|")[0] == tbin:
            for b in bins:
                ctm[b] += binc.get(b, 0)
    if sum(ctm.values()) >= MIN_STRATUM:
        return smooth(ctm, bins), "journal:type"

    # level 3: pooled type marginal
    return smooth(pooled[tbin], bins), "pooled:type"


# ---------------------------------------------------------------------------
# Inference  (in genuine-space: LR favours genuine when < 1 it lowers it)
# ---------------------------------------------------------------------------

def gap_lr(gap, gdist, alpha):
    """P(b | genuine) / P(b | not_genuine); < 1 for a suspiciously fast gap."""
    b = gap["z_bin"]
    g = gdist[b]
    not_genuine = alpha * MANIP_DIST[b] + (1 - alpha) * g
    return g / not_genuine


def author_posterior(gaps, journals, pooled, bins, alpha, prior_genuine, want_detail=False):
    odds = prior_genuine / (1 - prior_genuine)
    detail = []
    for g in gaps:
        gdist, level = genuine_dist(journals, pooled, bins, g["journal_id"], g["type_bin"], g["pages_bin"])
        lr = gap_lr(g, gdist, alpha)
        odds *= lr
        if want_detail:
            detail.append((g, lr, level, gdist[g["z_bin"]]))
    return odds / (1 + odds), detail


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if not os.path.exists(IN_PATH):
        raise SystemExit(f"Run bbn_extract_v2.py first; {IN_PATH} not found.")
    with open(IN_PATH, encoding="utf-8") as f:
        data = json.load(f)

    bins = data["config"]["z_bins"]
    journals = data["journals"]
    suspects = data["suspects"]
    pooled = build_pooled(journals, bins)

    print(f"Model: {data['model']} | alpha={ALPHA} | prior P(genuine)={PRIOR_GENUINE} | MANIP_DIST={MANIP_DIST}")

    # --- detailed per-gap breakdown at the default settings ---------------
    print(f"\n=== PER-GAP CONTRIBUTIONS (alpha={ALPHA}, prior P(genuine)={PRIOR_GENUINE}) ===")
    print("  (LR<1 lowers genuineness; a genuine author's gaps match the peer baseline)")
    results = []
    for name, s in suspects.items():
        post, detail = author_posterior(s["gaps"], journals, pooled, bins, ALPHA, PRIOR_GENUINE, True)
        results.append((name, post, s["n_gaps"]))
        print(f"\n  {name!r}  ->  P(genuine) = {post:.3f}   "
              f"(n_gaps={s['n_gaps']}, confidence={confidence_band(s['n_gaps'])})")
        for g, lr, level, p_genuine in detail[:6]:
            print(f"     z={g['z']:>7} {g['z_bin']:<12} j={g['journal_id']:<7} "
                  f"genuine_p={p_genuine:.4f} [{level:<17}]  LR={lr:>6.3f}")
        if len(detail) > 6:
            extra = detail[6:]
            prod = 1.0
            for _, lr, _, _ in extra:
                prod *= lr
            print(f"     ... (+{len(extra)} more gaps, combined LR={prod:.3f})")

    print("\n=== INVESTIGATION PRIORITY (lowest genuineness first; NOT a guilt ordering) ===")
    print("  (identical suspicious gap shared by all; rank reflects how much clean")
    print("   record restores genuineness, plus how much record exists at all)")
    for name, post, n in sorted(results, key=lambda r: r[1]):
        print(f"  P(genuine)={post:.3f}  {name:<22} n_gaps={n:<3} confidence={confidence_band(n)}")

    # --- sweeps -----------------------------------------------------------
    names = [r[0] for r in results]
    short = {n: n.split()[-1] for n in names}

    print(f"\n=== ALPHA SENSITIVITY  (P(genuine), prior={PRIOR_GENUINE}) ===")
    print("  alpha  " + "  ".join(f"{short[n]:>11}" for n in names))
    for a in ALPHA_SWEEP:
        line = f"  {a:<6}"
        for n in names:
            p, _ = author_posterior(suspects[n]["gaps"], journals, pooled, bins, a, PRIOR_GENUINE)
            line += f"  {p:>11.3f}"
        print(line)

    print(f"\n=== PRIOR SENSITIVITY  (P(genuine), alpha={ALPHA}) ===")
    print("  P(gen)0 " + "  ".join(f"{short[n]:>11}" for n in names))
    for pr in PRIOR_GENUINE_SWEEP:
        line = f"  {pr:<6}"
        for n in names:
            p, _ = author_posterior(suspects[n]["gaps"], journals, pooled, bins, ALPHA, pr)
            line += f"  {p:>11.3f}"
        print(line)


if __name__ == "__main__":
    main()
