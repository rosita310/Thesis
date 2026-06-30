"""
pgmpy cross-check of the hand-rolled V2 inference.

Builds the SAME per-author Bayesian network in pgmpy -- G = is_genuine as the
latent root, one observed gap node per gap, each gap's CPT being the journal-
specific genuine column for G=genuine and the alpha-mixture for G=not_genuine --
runs exact VariableElimination, and prints the pgmpy posterior P(genuine) beside
the hand-rolled one. They should agree to ~1e-9.

This validates the claim that bbn_infer_v2.py's closed form
    posterior_odds(genuine) = prior_odds(genuine) * PRODUCT_gaps LR(gap)
IS exact Bayesian-network inference on this structure (a naive-Bayes polytree),
not an approximation.

CPTs and helpers are imported from bbn_infer_v2.py, so BOTH sides use byte-for-byte
identical numbers; any discrepancy would therefore be inference-only.

pgmpy pulls numpy/scipy/pandas (and, in recent versions, torch). Install it in a
SEPARATE analysis venv -- NOT the scraper venv:
    python -m venv .venv_analysis
    .venv_analysis\\Scripts\\activate
    pip install pgmpy
    python bbn_infer_v2_pgmpy.py            # full cross-check (needs the extract JSON)
    python bbn_infer_v2_pgmpy.py --selftest # API/agreement check, no DB/JSON needed
"""

from __future__ import annotations

import json
import os
import sys

import solution.SelfReview.Springer.BBN.archive.bbn_infer_v2 as v2

try:
    try:
        from pgmpy.models import DiscreteBayesianNetwork as BN   # pgmpy >= 1.0
    except ImportError:
        from pgmpy.models import BayesianNetwork as BN           # pgmpy < 1.0
    from pgmpy.factors.discrete import TabularCPD
    from pgmpy.inference import VariableElimination
except ImportError:
    sys.exit("pgmpy not installed. In a separate analysis venv:  pip install pgmpy")


# ---------------------------------------------------------------------------
# Build the per-author network and query P(G=genuine | all gaps)
# ---------------------------------------------------------------------------

def not_genuine_column(genuine, alpha):
    """alpha-mixture not-genuine column for one gap: alpha*manip + (1-alpha)*genuine."""
    return {b: alpha * v2.MANIP_DIST[b] + (1 - alpha) * genuine[b] for b in genuine}


def pgmpy_query(genuine_columns, observed_bins, alpha, prior_genuine, bins):
    """genuine_columns: list of P(b|genuine) dicts (one per gap); observed_bins: their z-bins."""
    model = BN()
    model.add_node("G")
    # G = is_genuine; state order [genuine, not_genuine]
    cpds = [TabularCPD("G", 2, [[prior_genuine], [1 - prior_genuine]],
                       state_names={"G": ["genuine", "not_genuine"]})]
    for i, genuine in enumerate(genuine_columns):
        ng = not_genuine_column(genuine, alpha)
        node = f"g{i}"
        model.add_edge("G", node)
        # rows = gap bins (in `bins` order); columns = G states [genuine, not_genuine]
        values = [[genuine[b], ng[b]] for b in bins]
        cpds.append(TabularCPD(node, len(bins), values,
                               evidence=["G"], evidence_card=[2],
                               state_names={node: bins, "G": ["genuine", "not_genuine"]}))
    model.add_cpds(*cpds)
    model.check_model()

    infer = VariableElimination(model)
    evidence = {f"g{i}": b for i, b in enumerate(observed_bins)}
    try:
        q = infer.query(["G"], evidence=evidence, show_progress=False)
    except TypeError:                       # older pgmpy without show_progress
        q = infer.query(["G"], evidence=evidence)
    return float(q.values[q.state_names["G"].index("genuine")])


def hand_one_author(genuine_columns, observed_bins, alpha, prior_genuine, bins):
    """Hand-rolled P(genuine) using the same LR formula as bbn_infer_v2."""
    odds = prior_genuine / (1 - prior_genuine)
    for genuine, b in zip(genuine_columns, observed_bins):
        ng = alpha * v2.MANIP_DIST[b] + (1 - alpha) * genuine[b]
        odds *= genuine[b] / ng
    return odds / (1 + odds)


# ---------------------------------------------------------------------------
# Self-test (no DB / JSON needed) -- proves pgmpy API + agreement on a toy case
# ---------------------------------------------------------------------------

def selftest():
    bins = ["typical", "mild_fast", "extreme", "very_extreme"]
    genuine = {"typical": 0.90, "mild_fast": 0.06, "extreme": 0.03, "very_extreme": 0.01}
    alpha, prior_genuine = 0.10, 0.95
    cols = [genuine, genuine]               # two gaps
    obs = ["very_extreme", "typical"]       # one suspicious, one normal
    hand = hand_one_author(cols, obs, alpha, prior_genuine, bins)
    pg = pgmpy_query(cols, obs, alpha, prior_genuine, bins)
    print(f"SELFTEST  P(genuine): hand={hand:.9f}  pgmpy={pg:.9f}  |diff|={abs(hand-pg):.2e}  "
          f"-> {'MATCH' if abs(hand - pg) < 1e-6 else 'MISMATCH'}")


# ---------------------------------------------------------------------------
# Main: cross-check the three suspects against the hand-rolled inference
# ---------------------------------------------------------------------------

def main():
    if "--selftest" in sys.argv:
        selftest()
        return

    if not os.path.exists(v2.IN_PATH):
        raise SystemExit(f"Run bbn_extract_v2.py first; {v2.IN_PATH} not found "
                         f"(or use --selftest).")
    with open(v2.IN_PATH, encoding="utf-8") as f:
        data = json.load(f)
    bins = data["config"]["z_bins"]
    journals = data["journals"]
    suspects = data["suspects"]
    pooled = v2.build_pooled(journals, bins)
    alpha, prior_genuine = v2.ALPHA, v2.PRIOR_GENUINE

    print(f"Cross-check vs pgmpy ({BN.__name__}) | P(genuine) | alpha={alpha} prior={prior_genuine}\n")
    print(f"  {'author':<22} {'hand':>11} {'pgmpy':>11} {'|diff|':>10}")
    maxdiff = 0.0
    for name, s in suspects.items():
        if not s["gaps"]:
            continue
        cols = [v2.genuine_dist(journals, pooled, bins, g["journal_id"],
                                g["type_bin"], g["pages_bin"])[0] for g in s["gaps"]]
        obs = [g["z_bin"] for g in s["gaps"]]
        # hand side uses the actual module function, so this validates the real code path
        hand, _ = v2.author_posterior(s["gaps"], journals, pooled, bins, alpha, prior_genuine)
        pg = pgmpy_query(cols, obs, alpha, prior_genuine, bins)
        maxdiff = max(maxdiff, abs(hand - pg))
        print(f"  {name:<22} {hand:>11.6f} {pg:>11.6f} {abs(hand - pg):>10.2e}")
    print(f"\n  max |diff| = {maxdiff:.2e}  -> "
          f"{'MATCH (exact inference confirmed)' if maxdiff < 1e-6 else 'MISMATCH - investigate'}")


if __name__ == "__main__":
    main()
