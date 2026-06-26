"""
BBN V3 inference for the self-review case study (SQ1.1) -- per-gap plate model,
author-centric and corpus-wide.

Latent node G = is_genuine in {genuine, not_genuine} (presumption of innocence:
the named state is innocence; we escalate cases whose genuineness is implausibly
low). Reads bbn_baselines/bbn_v3_corpus.json (from bbn_extract_v3.py) and, for
every author with at least one non-`typical` gap, multiplies the per-gap
likelihood ratios:

    posterior_odds(genuine) = prior_odds(genuine) * PRODUCT_over_gaps  LR(gap_i)

    LR(gap) = P(b | genuine, journal, type, pages) / P(b | not_genuine, ...)
            = genuine_b / ( alpha * P(b | manipulated) + (1 - alpha) * genuine_b )

where b is the gap's z-bin. genuine_b is the JOURNAL-SPECIFIC empirical baseline.
The not_genuine branch is the alpha-mixture: a fraction alpha of a non-genuine
author's papers are manipulated (-> near-instant review, MANIP_DIST), the rest
behave genuinely. A fast gap therefore yields LR < 1 and *lowers* genuineness.

LEAVE-ONE-AUTHOR (V3): when scoring author A, A's own papers are subtracted from
the baseline histogram A is compared against (at whichever back-off level is
used). An author is judged against peers, never against themselves -- this
removes the self-justification bias that would otherwise inflate genuineness for
exactly the most anomalous authors.

Genuine-baseline back-off ladder (so no hard zeros, pages=unknown is missing):
    journal type+pages  ->  journal type (pages marginal)  ->  pooled type
all Laplace-smoothed; the threshold check uses the POST-exclusion count.

Output: P(is_genuine | evidence) per author; investigation priority = lowest
genuineness first. A ranking to support manual review, never an accusation.

Run from this directory (BBN/), any venv (stdlib only):
    python bbn_infer_v3.py                  # score the corpus
    python bbn_infer_v3.py --in bbn_baselines/bbn_v3_corpus_10623.json
    python bbn_infer_v3.py --selftest       # synthetic validation, no data file
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
from collections import defaultdict

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

DEFAULT_IN = os.path.join(os.path.dirname(__file__), "bbn_baselines", "bbn_v3_corpus.json")
OUT_DIR = os.path.join(os.path.dirname(__file__), "bbn_baselines")

PRIOR_GENUINE = 0.95
PRIOR_GENUINE_SWEEP = [0.99, 0.98, 0.95, 0.90, 0.80]

ALPHA = 0.10                              # fraction of a non-genuine author's papers that are manipulated
ALPHA_SWEEP = [0.02, 0.05, 0.10, 0.25, 0.50]

# What a manipulated paper's gap looks like (the not_genuine component) -- the one
# elicited piece. Keys must match z_bins; sums to 1. Mass concentrates on the deep
# tail (manipulation -> near-instant review); mild_fast is kept above its ~0.10
# genuine rate so chronic-mild behavior still accumulates. typical=0 makes
# LR_typical = 1/(1-alpha). Sensitivity is reported via the alpha sweep.
MANIP_DIST = {"typical": 0.0, "mild_fast": 0.15, "extreme": 0.20,
              "very_extreme": 0.25, "ultra_extreme": 0.40}

LAPLACE = 0.5
MIN_STRATUM = 30                          # papers needed (post-exclusion) to trust a journal stratum
SHORTLIST_THRESHOLD = 0.50                # P(genuine) below this -> reported shortlist + full evidence breakdown
SWEEP_PRINT_CAP = 12                      # cap printed sensitivity rows


def sigmoid(log_odds):
    """P(genuine) from log-odds, overflow-safe. Underflows to 0.0 only when the true
    posterior is below ~1e-308 -- report weight-of-evidence (log_odds) in that regime."""
    if log_odds >= 0:
        return 1.0 / (1.0 + math.exp(-log_odds))
    e = math.exp(log_odds)
    return e / (1.0 + e)


def fmt_p(p):
    """Never render a non-zero posterior as a misleading 0.000."""
    return f"{p:.4f}" if p >= 1e-4 else f"{p:.2e}"


# ---------------------------------------------------------------------------
# Genuine baseline with leave-one-author exclusion + back-off
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


def _subtract(counts, own, match, bins):
    """Return a fresh {bin: count} = counts minus the author's matching papers."""
    out = {b: counts.get(b, 0) for b in bins}
    for p in own:
        if match(p):
            out[p["z_bin"]] = out.get(p["z_bin"], 0) - 1
    return out


def genuine_dist(journals, pooled, bins, jid, tbin, pbin, own):
    """P(bin | genuine, journal, type, pages) with author `own` papers removed.

    Returns (smoothed_dist, level_label) via back-off; threshold checks use the
    post-exclusion counts.
    """
    jbase = journals.get(jid, {}).get("baseline_counts", {})

    # level 1: journal-specific type+pages (only if pages known)
    if pbin not in ("unknown", None):
        c = jbase.get(f"{tbin}|{pbin}")
        if c:
            c = _subtract(c, own,
                          lambda p: p["journal_id"] == jid and p["type_bin"] == tbin
                          and p["pages_bin"] == pbin, bins)
            if sum(c.values()) >= MIN_STRATUM:
                return smooth(c, bins), "journal:type+pages"

    # level 2: journal type marginal (sum over pages)
    ctm = defaultdict(int)
    for sk, binc in jbase.items():
        if sk.split("|")[0] == tbin:
            for b in bins:
                ctm[b] += binc.get(b, 0)
    ctm = _subtract(ctm, own,
                    lambda p: p["journal_id"] == jid and p["type_bin"] == tbin, bins)
    if sum(ctm.values()) >= MIN_STRATUM:
        return smooth(ctm, bins), "journal:type"

    # level 3: pooled type marginal (final fallback, returned regardless of count)
    cp = _subtract(pooled.get(tbin, {}), own, lambda p: p["type_bin"] == tbin, bins)
    return smooth(cp, bins), "pooled:type"


# ---------------------------------------------------------------------------
# Inference  (genuine-space: LR < 1 lowers genuineness)
# ---------------------------------------------------------------------------

def gap_lr(z_bin, gdist, alpha):
    g = gdist[z_bin]
    not_genuine = alpha * MANIP_DIST[z_bin] + (1 - alpha) * g
    return g / not_genuine


def score_author(gaps, journals, pooled, bins, alpha, prior_genuine, want_detail=False):
    """gaps = the author's full list of usable papers (typical ones restore genuineness).

    Accumulated in log-space (weight of evidence, Good 1985): log_odds = log prior_odds
    + sum log LR. Returns (P(genuine), log_odds, detail). log_odds is the robust signal;
    P can underflow to 0.0 for extreme cases while log_odds stays finite and ordered.
    """
    log_odds = math.log(prior_genuine / (1 - prior_genuine))
    detail = []
    for g in gaps:
        gdist, level = genuine_dist(journals, pooled, bins,
                                    g["journal_id"], g["type_bin"], g["pages_bin"], gaps)
        lr = gap_lr(g["z_bin"], gdist, alpha)
        log_odds += math.log(lr)
        if want_detail:
            detail.append({
                "doi": g.get("doi"), "journal_id": g["journal_id"],
                "z": g.get("z"), "z_bin": g["z_bin"],
                "type_bin": g["type_bin"], "pages_bin": g["pages_bin"],
                "backoff_level": level, "genuine_p": round(gdist[g["z_bin"]], 6),
                "lr": round(lr, 6),
            })
    return sigmoid(log_odds), log_odds, detail


def iter_candidates(data):
    """Yield (name, gaps) for every author with >=1 non-typical gap."""
    papers = data["papers"]
    for name, dois in data["author_index"].items():
        gaps = [{**papers[d], "doi": d} for d in dois if d in papers]
        if any(g["z_bin"] != "typical" for g in gaps):
            yield name, gaps


def rank_corpus(data, alpha=ALPHA, prior=PRIOR_GENUINE):
    """Return rows sorted by P(genuine) ascending; each row carries its gaps."""
    bins = data["config"]["z_bins"]
    journals = data["journals"]
    pooled = build_pooled(journals, bins)
    rows = []
    for name, gaps in iter_candidates(data):
        post, log_odds, _ = score_author(gaps, journals, pooled, bins, alpha, prior)
        nontyp = sum(1 for g in gaps if g["z_bin"] != "typical")
        worst = min(gaps, key=lambda g: g["z"])
        rows.append({
            "name": name, "p_genuine": post, "log10_odds": log_odds / math.log(10),
            "n_gaps": len(gaps), "n_nontypical": nontyp,
            "n_journals": len({g["journal_id"] for g in gaps}),
            "lowest_z": worst["z"], "lowest_bin": worst["z_bin"], "gaps": gaps,
        })
    rows.sort(key=lambda r: r["log10_odds"])   # weight of evidence: orders even when P underflows
    return rows, journals, pooled, bins


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="BBN V3 author-centric inference.")
    parser.add_argument("--in", dest="in_path", default=DEFAULT_IN, help="corpus JSON from extract")
    parser.add_argument("--threshold", type=float, default=SHORTLIST_THRESHOLD,
                        help="P(genuine) below which an author is shortlisted")
    parser.add_argument("--selftest", action="store_true", help="run synthetic validation and exit")
    args = parser.parse_args()

    if args.selftest:
        return selftest()

    if not os.path.exists(args.in_path):
        raise SystemExit(f"Run bbn_extract_v3.py first; {args.in_path} not found.")
    with open(args.in_path, encoding="utf-8") as f:
        data = json.load(f)

    assert set(MANIP_DIST) == set(data["config"]["z_bins"]), "MANIP_DIST keys must match z_bins"

    rows, journals, pooled, bins = rank_corpus(data, ALPHA, PRIOR_GENUINE)
    shortlist = [r for r in rows if r["p_genuine"] < args.threshold]

    print(f"Model: {data['model']} | alpha={ALPHA} | prior P(genuine)={PRIOR_GENUINE} | MANIP_DIST={MANIP_DIST}")
    print(f"Scored {len(rows)} authors (>=1 non-typical gap); "
          f"{len(shortlist)} below threshold {args.threshold}.")

    # --- ranking CSV (all scored authors) ---------------------------------
    # log10_odds (weight of evidence) is the primary, non-saturating sort key;
    # p_genuine is written with full precision so tiny posteriors never read 0.
    rank_path = os.path.join(OUT_DIR, "bbn_v3_ranking.csv")
    cols = ["name", "p_genuine", "log10_odds", "n_gaps", "n_nontypical", "n_journals",
            "lowest_z", "lowest_bin"]
    with open(rank_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            out = dict(r)
            out["p_genuine"] = f"{r['p_genuine']:.6g}"
            out["log10_odds"] = round(r["log10_odds"], 3)
            w.writerow({c: out[c] for c in cols})
    print(f"Wrote {rank_path}")

    # --- shortlist JSON (full per-gap evidence breakdown) -----------------
    shortlist_out = []
    for r in shortlist:
        _, _, detail = score_author(r["gaps"], journals, pooled, bins, ALPHA, PRIOR_GENUINE, True)
        detail.sort(key=lambda d: d["lr"])  # most incriminating gap first
        shortlist_out.append({
            "name": r["name"], "p_genuine": r["p_genuine"],
            "log10_odds": round(r["log10_odds"], 3),
            "n_gaps": r["n_gaps"], "n_nontypical": r["n_nontypical"],
            "n_journals": r["n_journals"],
            "gaps": detail,
        })
    sl_path = os.path.join(OUT_DIR, "bbn_v3_shortlist.json")
    with open(sl_path, "w", encoding="utf-8") as f:
        json.dump({"alpha": ALPHA, "prior_genuine": PRIOR_GENUINE,
                   "threshold": args.threshold, "manip_dist": MANIP_DIST,
                   "n_shortlisted": len(shortlist_out), "authors": shortlist_out},
                  f, ensure_ascii=False, indent=2)
    print(f"Wrote {sl_path}")

    # --- console: shortlist ranking + a sample evidence breakdown ---------
    print("\n=== INVESTIGATION PRIORITY (lowest genuineness first; NOT a guilt ordering) ===")
    print("  (WoE = log10 posterior-odds of genuine; more negative = stronger evidence against."
          " P can underflow but WoE stays ordered. Absolute values are overconfident with large n"
          " -- the ranking + sensitivity are the reportable output.)")
    for r in shortlist:
        print(f"  P(genuine)={fmt_p(r['p_genuine']):>9}  WoE={r['log10_odds']:>7.2f}  {r['name']:<28} "
              f"n_gaps={r['n_gaps']:<3} non-typ={r['n_nontypical']:<3} "
              f"j={r['n_journals']:<2} worst_z={r['lowest_z']:>7} [{r['lowest_bin']}]")

    if shortlist_out:
        top = shortlist_out[0]
        print(f"\n=== EVIDENCE BREAKDOWN (lowest-scoring author: {top['name']!r}) ===")
        print("  (LR<1 lowers genuineness; a genuine author's gaps match the peer baseline)")
        for d in top["gaps"][:10]:
            print(f"     z={d['z']:>7} {d['z_bin']:<12} j={d['journal_id']:<7} "
                  f"genuine_p={d['genuine_p']:.4f} [{d['backoff_level']:<17}] LR={d['lr']:>7.3f}")
        if len(top["gaps"]) > 10:
            print(f"     ... (+{len(top['gaps']) - 10} more gaps)")

    # --- sensitivity sweeps on the shortlist ------------------------------
    if shortlist:
        sweep = shortlist[:SWEEP_PRINT_CAP]
        labels = [r["name"].split()[-1][:11] for r in sweep]
        print(f"\n=== ALPHA SENSITIVITY  (P(genuine), prior={PRIOR_GENUINE}; top {len(sweep)} shortlist) ===")
        print("  alpha  " + "  ".join(f"{l:>11}" for l in labels))
        for a in ALPHA_SWEEP:
            line = f"  {a:<6}"
            for r in sweep:
                p = score_author(r["gaps"], journals, pooled, bins, a, PRIOR_GENUINE)[0]
                line += f"  {fmt_p(p):>11}"
            print(line)

        print(f"\n=== PRIOR SENSITIVITY  (P(genuine), alpha={ALPHA}; top {len(sweep)} shortlist) ===")
        print("  P(gen)0 " + "  ".join(f"{l:>11}" for l in labels))
        for pr in PRIOR_GENUINE_SWEEP:
            line = f"  {pr:<6}"
            for r in sweep:
                p = score_author(r["gaps"], journals, pooled, bins, ALPHA, pr)[0]
                line += f"  {fmt_p(p):>11}"
            print(line)


# ---------------------------------------------------------------------------
# Self-test (synthetic corpus; no DB / data file needed)
# ---------------------------------------------------------------------------

def _approx(a, b, tol=1e-4):
    return abs(a - b) <= tol


def selftest():
    bins = ["typical", "mild_fast", "extreme", "very_extreme", "ultra_extreme"]
    # Journal J1: author papers are INCLUDED in the baseline (as extract emits them);
    # leave-one-author exclusion subtracts them back out at scoring time.
    journals = {
        "J1": {"baseline_counts": {
            "normal_type|normal": {"typical": 80, "mild_fast": 10, "extreme": 6,
                                   "very_extreme": 3, "ultra_extreme": 1},
            "fast_type|short":    {"typical": 18, "mild_fast": 5, "extreme": 3,
                                   "very_extreme": 2, "ultra_extreme": 2},
        }},
    }
    pooled = build_pooled(journals, bins)
    P = lambda g, a=ALPHA, pr=PRIOR_GENUINE: score_author(g, journals, pooled, bins, a, pr)[0]

    def gap(zb, z, doi, t="normal_type", p="normal"):
        return {"journal_id": "J1", "type_bin": t, "pages_bin": p, "z_bin": zb, "z": z, "doi": doi}

    A = [gap("very_extreme", -5.0, "A1")]
    C = [gap("typical", 0.2, "C1")]
    D = [gap("extreme", -3.0, "D1", t="fast_type", p="short")]
    E = [gap("ultra_extreme", -9.0, "E1")]

    ok = True
    def check(name, cond):
        nonlocal ok
        print(f"  [{'PASS' if cond else 'FAIL'}] {name}")
        ok = ok and cond

    # 1. leave-one-author posterior for a single very_extreme paper (hand-computed,
    #    5 bins, MANIP very_extreme=0.25). LOO: ve 3->2, total 99>=30 -> level1;
    #    smooth denom 99+0.5*5=101.5; g=(2+.5)/101.5=.0246305;
    #    not_g=.1*.25+.9*g=.0471675; LR=.522197; odds=19*LR=9.92174; P=.908438
    pA = P(A)
    check(f"LOO very_extreme posterior == 0.9084  (got {pA:.6f})", _approx(pA, 0.908438))

    # 2. without exclusion the same author looks MORE genuine
    #    g=(3+.5)/(100+2.5)=.0341463; not_g=.025+.9*g=.0557317; LR=.612691; P=.920895
    g0, _ = genuine_dist(journals, pooled, bins, "J1", "normal_type", "normal", [])
    lr0 = gap_lr("very_extreme", g0, ALPHA)
    odds0 = (PRIOR_GENUINE / (1 - PRIOR_GENUINE)) * lr0
    p_noloo = odds0 / (1 + odds0)
    check(f"no-exclusion posterior == 0.9209  (got {p_noloo:.6f})", _approx(p_noloo, 0.920895))
    check("leave-one-author LOWERS genuineness for the anomaly", pA < p_noloo)

    # 3. finer tail separates magnitude: an ultra_extreme gap is far more incriminating
    #    than a very_extreme one in the SAME stratum (the whole point of the p0.1 cut).
    g_ve, _ = genuine_dist(journals, pooled, bins, "J1", "normal_type", "normal", A)
    g_ue, _ = genuine_dist(journals, pooled, bins, "J1", "normal_type", "normal", E)
    lr_ve = gap_lr("very_extreme", g_ve, ALPHA)
    lr_ue = gap_lr("ultra_extreme", g_ue, ALPHA)
    check(f"LR_ultra ({lr_ue:.3f}) < LR_very_extreme ({lr_ve:.3f})", lr_ue < lr_ve)
    check(f"ultra_extreme author scored more suspicious than very_extreme  "
          f"(P {P(E):.3f} < {P(A):.3f})", P(E) < P(A))

    # 4. LR_typical == 1/(1-alpha) exactly, independent of counts/back-off
    gt, _ = genuine_dist(journals, pooled, bins, "J1", "normal_type", "normal", C)
    lrt = gap_lr("typical", gt, ALPHA)
    check(f"LR_typical == 1/(1-alpha)=1.11111  (got {lrt:.8f})", _approx(lrt, 1 / (1 - ALPHA), 1e-9))

    # 5. a typical-only author is NOT a candidate; anomalous ones are
    data = {"papers": {"A1": A[0], "C1": C[0], "D1": D[0], "E1": E[0]},
            "author_index": {"AuthA": ["A1"], "AuthC": ["C1"], "AuthD": ["D1"], "AuthE": ["E1"]}}
    cand = {n for n, _ in iter_candidates(data)}
    check("candidate set = {AuthA, AuthD, AuthE} (typical-only AuthC excluded)",
          cand == {"AuthA", "AuthD", "AuthE"})

    # 6. full back-off chain with post-exclusion threshold:
    #    D's fast_type|short has 30; minus D's paper -> 29 < 30 -> journal:type (29) < 30
    #    -> pooled:type (final fallback)
    _, lvlD = genuine_dist(journals, pooled, bins, "J1", "fast_type", "short", D)
    check(f"back-off falls through to pooled:type  (got {lvlD})", lvlD == "pooled:type")

    # 7. ranking orders ascending by P(genuine) (= ascending weight of evidence)
    rows, *_ = rank_corpus({"config": {"z_bins": bins}, "journals": journals,
                            "papers": data["papers"], "author_index": data["author_index"]})
    ps = [r["p_genuine"] for r in rows]
    check("ranking is sorted ascending by P(genuine)", ps == sorted(ps))

    print("\nSELFTEST:", "ALL PASS" if ok else "FAILURES PRESENT")
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
