"""
BBN V2 inference for the self-review case study (SQ1.1) -- per-gap plate model.

Reads bbn_baselines/bbn_v2_baseline_<journal>.json (from bbn_extract_v2.py) and,
for each investigated author, multiplies the per-gap likelihood ratios:

    posterior_odds = prior_odds * PRODUCT_over_gaps  LR(gap_i)

    LR(gap) = P(b | fraud, journal, type, pages) / P(b | honest, journal, type, pages)
            = (1 - alpha) + alpha * P(b | manipulated) / P(b | honest, journal, type, pages)

where b is the gap's z-bin. The honest term is the JOURNAL-SPECIFIC empirical
baseline (each gap judged by its own journal). The fraud term is the
alpha-mixture: with prob alpha the paper was manipulated (-> near-instant review,
MANIP_DIST), otherwise it behaves like an honest paper in that journal.

Only alpha and MANIP_DIST are elicited; the type/pages influence is entirely in
the empirical honest denominator.

Honest-baseline back-off ladder (so no hard zeros, and pages=unknown is treated
as missing rather than a category):
    journal type+pages  ->  journal type (pages marginal)  ->  pooled type
all Laplace-smoothed.

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

PRIOR_FRAUD = 0.05
PRIOR_SWEEP = [0.01, 0.02, 0.05, 0.10, 0.20]

ALPHA = 0.10                              # fraction of papers a fraudster manipulates
ALPHA_SWEEP = [0.02, 0.05, 0.10, 0.25, 0.50]

# What a manipulated paper's gap looks like. Most mass on the extreme bins
# (self-review is usually a big speedup); mild_fast is now armed above the ~0.13
# honest rate so a chronically slightly-fast author accumulates evidence (at the
# cost of specificity vs an efficient honest author). Keys must match z_bins; sums to 1.
MANIP_DIST = {"typical": 0.0, "mild_fast": 0.25, "extreme": 0.25, "very_extreme": 0.50}

LAPLACE = 0.5
MIN_STRATUM = 30                          # need this many papers to trust a journal stratum


def confidence_band(n_gaps):
    """How much record we have to contextualize the verdict (NOT strength of guilt)."""
    if n_gaps <= 2:  return "low"
    if n_gaps <= 9:  return "medium"
    return "high"


# ---------------------------------------------------------------------------
# Honest baseline with back-off
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


def honest_dist(journals, pooled, bins, jid, tbin, pbin):
    """Return (smoothed distribution, level-label) via the back-off ladder."""
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
# Inference
# ---------------------------------------------------------------------------

def gap_lr(gap, honest, alpha):
    b = gap["z_bin"]
    denom = honest[b]
    return (1 - alpha) + alpha * MANIP_DIST[b] / denom


def author_posterior(gaps, journals, pooled, bins, alpha, prior, want_detail=False):
    odds = prior / (1 - prior)
    detail = []
    for g in gaps:
        honest, level = honest_dist(journals, pooled, bins, g["journal_id"], g["type_bin"], g["pages_bin"])
        lr = gap_lr(g, honest, alpha)
        odds *= lr
        if want_detail:
            detail.append((g, lr, level, honest[g["z_bin"]]))
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

    print(f"Model: {data['model']} | alpha={ALPHA} | prior={PRIOR_FRAUD} | MANIP_DIST={MANIP_DIST}")

    # --- detailed per-gap breakdown at the default settings ---------------
    print(f"\n=== PER-GAP CONTRIBUTIONS (alpha={ALPHA}, prior={PRIOR_FRAUD}) ===")
    results = []
    for name, s in suspects.items():
        post, detail = author_posterior(s["gaps"], journals, pooled, bins, ALPHA, PRIOR_FRAUD, True)
        results.append((name, post, s["n_gaps"]))
        print(f"\n  {name!r}  ->  P(fraud) = {post:.3f}   "
              f"(n_gaps={s['n_gaps']}, confidence={confidence_band(s['n_gaps'])})")
        for g, lr, level, p_honest in detail[:6]:
            print(f"     z={g['z']:>7} {g['z_bin']:<12} j={g['journal_id']:<7} "
                  f"honest_p={p_honest:.4f} [{level:<17}]  LR={lr:>7.2f}")
        if len(detail) > 6:
            extra = detail[6:]
            prod = 1.0
            for _, lr, _, _ in extra:
                prod *= lr
            print(f"     ... (+{len(extra)} more gaps, combined LR={prod:.3f})")

    print("\n=== RANKING (investigation priority; NOT a guilt ordering) ===")
    print("  (identical incriminating gap shared by all; rank reflects how much")
    print("   clean record exonerates each, plus how much record exists at all)")
    for name, post, n in sorted(results, key=lambda r: -r[1]):
        print(f"  {post:.3f}  {name:<22} n_gaps={n:<3} confidence={confidence_band(n)}")

    # --- sweeps -----------------------------------------------------------
    names = [r[0] for r in results]
    short = {n: n.split()[-1] for n in names}

    print(f"\n=== ALPHA SENSITIVITY (prior={PRIOR_FRAUD}) ===")
    print("  alpha  " + "  ".join(f"{short[n]:>11}" for n in names))
    for a in ALPHA_SWEEP:
        line = f"  {a:<6}"
        for n in names:
            p, _ = author_posterior(suspects[n]["gaps"], journals, pooled, bins, a, PRIOR_FRAUD)
            line += f"  {p:>11.3f}"
        print(line)

    print(f"\n=== PRIOR SENSITIVITY (alpha={ALPHA}) ===")
    print("  prior  " + "  ".join(f"{short[n]:>11}" for n in names))
    for pr in PRIOR_SWEEP:
        line = f"  {pr:<6}"
        for n in names:
            p, _ = author_posterior(suspects[n]["gaps"], journals, pooled, bins, ALPHA, pr)
            line += f"  {p:>11.3f}"
        print(line)


if __name__ == "__main__":
    main()
