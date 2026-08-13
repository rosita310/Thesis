"""
BBN inference for the self-review case study (SQ1.1).

Reads the corpus JSON from bbn_extract_v3.py and scores every author who has at
least one paper with z < CANDIDATE_Z_THRESHOLD (default -3.0). This threshold
matches Westerbaan's original outlier definition for this attack (an author is
flagged as soon as one of their papers has a z-score below -3). V3 flagged an
author on ANY non-typical gap (i.e. beyond the corpus's p15 percentile edge,
~-0.91); V4 narrows candidacy to Westerbaan's fixed z<-3 cut so the author pool
we score lines up with the outlier list his method would produce. Once an
author is flagged, nothing else changes: ALL of their gaps (not just the one
that crossed -3) are still scored as evidence, so we can tell whether the fast
gap is an isolated event or part of a structural, recurring pattern, and
whether it is explained away by a benign covariate (page count, article type)
via the journal/type/page-conditioned CPT.

The latent node is G = is_genuine in {genuine, not_genuine} (presumption of
innocence): we report P(genuine | evidence) and escalate the LOWEST values for
manual review -- a ranking to support investigation, never an accusation.

Each gap contributes a likelihood ratio:

    LR(gap) = P(b | genuine, journal, type, pages) / P(b | not_genuine, ...)
            = genuine_b / ( alpha * MANIP_DIST[b] + (1 - alpha) * genuine_b )

where b is the gap's z-bin and genuine_b is the journal-specific empirical
baseline. The not_genuine branch is an alpha-mixture: a fraction `alpha` of a
non-genuine author's papers are manipulated (near-instant review, distribution
MANIP_DIST), the rest behave genuinely. A fast gap gives LR < 1 and lowers
genuineness; MANIP_DIST[typical] = 0 makes LR_typical = 1/(1-alpha), so a typical
gap is mild positive evidence.

Combined in log space (weight of evidence, additive over gaps):
    log odds(genuine|E) = log odds(genuine) + SUM log LR_i
A very small posterior is reported as log10-odds (never a misleading 0.000), and
the ranking is by weight of evidence. Absolute posteriors are overconfident at
large n (gaps are not perfectly independent), so lean on the ranking and the
sensitivity sweeps: alpha, prior, the manipulation shape m_b, and the outer
z-bin edge (the mild_fast/typical boundary, swept over p10..p25 -- the "why p15
and not p25" question -- by re-binning the corpus and re-ranking).

Leave-one-author: when scoring author A, A's own papers are subtracted from the
baseline A is compared against -- judged against peers, not itself. Genuine
baseline uses a Laplace-smoothed back-off (threshold checked on post-exclusion
counts); pages=unknown is treated as missing:
    journal type+pages  ->  journal type (pages marginal)  ->  pooled type

Outputs (bbn_baselines/): bbn_v4_ranking.csv (all scored authors, summary rows)
and bbn_v4_scored.json (ALL scored authors with the full per-gap evidence
breakdown, most-suspicious first; each carries a `shortlisted` flag for the
P(genuine) < threshold subset). The threshold governs only the terminal output,
never what is written to disk.

Run from this directory (BBN/), any venv (stdlib only):
    python bbn_infer_v4.py                  # score the corpus
    python bbn_infer_v4.py --in bbn_baselines/bbn_v3_corpus_10623.json
    python bbn_infer_v4.py --selftest       # synthetic validation, no data file
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
from collections import defaultdict

_ORCID_RE = re.compile(r"^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$")


def _is_orcid(s):
    return bool(_ORCID_RE.match(s or ""))

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

DEFAULT_IN = os.path.join(os.path.dirname(__file__), "bbn_baselines", "bbn_v3_corpus.json")
OUT_DIR = os.path.join(os.path.dirname(__file__), "bbn_baselines")

CANDIDATE_Z_THRESHOLD = -3.0             # Westerbaan's outlier cut: >=1 paper with z below this
                                         # flags the author as a candidate for scoring.

PRIOR_GENUINE = 0.95
PRIOR_GENUINE_SWEEP = [0.99, 0.98, 0.95, 0.90, 0.80]

ALPHA = 0.20                             # fraction of a non-genuine author's papers that are manipulated.
                                         # Chronic-gamer target: ~1 in 5 papers gamed. A higher alpha both
                                         # matches "consistent gaming" and makes clean papers exonerate
                                         # harder (LR_typical = 1/(1-alpha)), so an occasional-fast author
                                         # with a long clean record drops off while a chronic one stays.
ALPHA_SWEEP = [0.10, 0.15, 0.20, 0.25, 0.30]

# What a manipulated paper's gap looks like (the not_genuine component) -- the one
# elicited piece. Keys must match z_bins; sums to 1. Tuned for the CHRONIC-gamer
# target: the peak sits on very_extreme (a consistently very-fast review), with
# ultra reduced so a single catastrophic gap cannot outrank a sustained pattern,
# and mild_fast kept ~neutral (near its ~0.10 genuine rate) so benign efficient
# authors are not swept in. typical=0 makes LR_typical = 1/(1-alpha). Sensitivity
# to this shape is reported via the m_b sweep.
MANIP_DIST = {"typical": 0.0, "mild_fast": 0.10, "extreme": 0.25,
              "very_extreme": 0.40, "ultra_extreme": 0.25}

# Alternative manipulation shapes for the m_b sensitivity sweep. Every shape puts
# zero mass on `typical` (manipulation never slows a review) and differs only in
# how the remaining mass is spread over the fast bins: `deep-tail` emphasizes a
# single catastrophic gap (one-off severe), `shallow` emphasizes a chronic run of
# mildly-fast gaps, `uniform` is agnostic. `baseline` is the chronic-tuned MANIP_DIST.
MANIP_SWEEP = {
    "baseline":  {"typical": 0.0, "mild_fast": 0.10, "extreme": 0.25, "very_extreme": 0.40, "ultra_extreme": 0.25},
    "deep-tail": {"typical": 0.0, "mild_fast": 0.05, "extreme": 0.15, "very_extreme": 0.30, "ultra_extreme": 0.50},
    "uniform":   {"typical": 0.0, "mild_fast": 0.25, "extreme": 0.25, "very_extreme": 0.25, "ultra_extreme": 0.25},
    "shallow":   {"typical": 0.0, "mild_fast": 0.40, "extreme": 0.30, "very_extreme": 0.20, "ultra_extreme": 0.10},
}

LAPLACE = 0.5
MIN_STRATUM = 30                          # papers needed (post-exclusion) to trust a journal stratum
SHORTLIST_THRESHOLD = 0.50                # P(genuine) below this -> reported shortlist + full evidence breakdown
SWEEP_PRINT_CAP = 12                      # cap printed sensitivity rows

# Percentiles to try for the OUTER z-bin edge (the mild_fast/typical boundary) in
# the discretization sensitivity sweep. 15 is the model's default (matches the
# extract's p15 cut); 10 and 25 are the neighbouring choices the thesis calls
# "similarly defensible". Only the outer edge moves -- the fast-tail cuts
# p0.1/p1/p5 stay fixed -- and candidacy (z < CANDIDATE_Z_THRESHOLD) is on the
# continuous z, so the candidate SET is identical across the sweep; only the
# genuine baseline and the exoneration weight of near-typical gaps change.
Z_OUTER_PCT_SWEEP = [10, 15, 20, 25]
Z_OUTER_PCT_DEFAULT = 15


def log_odds_to_p(log_odds):
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

def gap_lr(z_bin, gdist, alpha, manip=MANIP_DIST):
    g = gdist[z_bin]
    not_genuine = alpha * manip[z_bin] + (1 - alpha) * g
    return g / not_genuine


def score_author(gaps, journals, pooled, bins, alpha, prior_genuine, want_detail=False,
                 manip=MANIP_DIST):
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
        lr = gap_lr(g["z_bin"], gdist, alpha, manip)
        log_odds += math.log(lr)
        if want_detail:
            detail.append({
                "doi": g.get("doi"), "journal_id": g["journal_id"],
                "z": g.get("z"), "z_bin": g["z_bin"],
                "type_bin": g["type_bin"], "pages_bin": g["pages_bin"],
                "backoff_level": level, "genuine_p": round(gdist[g["z_bin"]], 6),
                "lr": round(lr, 6),
            })
    return log_odds_to_p(log_odds), log_odds, detail


def iter_candidates(data, z_threshold=CANDIDATE_Z_THRESHOLD):
    """Yield (identity, label, gaps) for every author with >=1 gap at z < z_threshold.

    Matches Westerbaan's outlier definition (a single paper with z < -3 flags the
    author). Once flagged, ALL of the author's usable papers are yielded as
    `gaps` -- the candidacy check only decides WHO gets scored, not what evidence
    is used to score them.
    """
    papers = data["papers"]
    labels = data.get("author_labels", {})
    for ident, dois in data["author_index"].items():
        gaps = [{**papers[d], "doi": d} for d in dois if d in papers]
        if any(g["z"] < z_threshold for g in gaps):
            yield ident, labels.get(ident, ident), gaps


def rank_corpus(data, alpha=ALPHA, prior=PRIOR_GENUINE, manip=MANIP_DIST,
                z_threshold=CANDIDATE_Z_THRESHOLD):
    """Return rows sorted by P(genuine) ascending; each row carries its gaps."""
    bins = data["config"]["z_bins"]
    journals = data["journals"]
    pooled = build_pooled(journals, bins)
    rows = []
    for ident, label, gaps in iter_candidates(data, z_threshold):
        post, log_odds, _ = score_author(gaps, journals, pooled, bins, alpha, prior, manip=manip)
        nontyp = sum(1 for g in gaps if g["z_bin"] != "typical")
        worst = min(gaps, key=lambda g: g["z"])
        rows.append({
            "ident": ident,
            "name": label, "orcid": ident if _is_orcid(ident) else "",
            "p_genuine": post, "log10_odds": log_odds / math.log(10),
            "n_gaps": len(gaps), "n_nontypical": nontyp,
            "n_journals": len({g["journal_id"] for g in gaps}),
            "lowest_z": worst["z"], "lowest_bin": worst["z_bin"], "gaps": gaps,
        })
    rows.sort(key=lambda r: r["log10_odds"])   # weight of evidence: orders even when P underflows
    return rows, journals, pooled, bins


# ---------------------------------------------------------------------------
# Re-binning for the discretization (typical-boundary) sensitivity sweep
# ---------------------------------------------------------------------------

def _edges_for_outer(config, outer_pct):
    """Return [(z_threshold, bin), ...] with the outer (mild_fast/typical) edge at
    `outer_pct`, reading the corpus's own pooled percentiles. The fast-tail cuts
    stay fixed at p0.1/p1/p5 (the extract's PCT_EDGES); only the outer edge moves.
    Raises KeyError if a needed percentile is absent from config.z_percentiles.
    """
    pct = config["z_percentiles"]
    return [(pct["p0.1"], "ultra_extreme"), (pct["p1"], "very_extreme"),
            (pct["p5"], "extreme"), (pct[f"p{outer_pct}"], "mild_fast"),
            (float("inf"), "typical")]


def _bin_of(z, edges):
    """z-bin for a continuous z under `edges` (ascending upper thresholds)."""
    for edge, label in edges:
        if z <= edge:
            return label
    return "typical"


def rebin_corpus(data, outer_pct):
    """A shallow copy of `data` with every paper re-binned under a different outer
    edge and the per-journal genuine baseline counts rebuilt to match.

    Everything is re-derived from each paper's continuous `z`, so the returned
    corpus is internally consistent (papers and baselines share one binning). The
    baseline is rebuilt from the `papers` dict, which the extract fills with every
    usable paper -- so this is faithful for a whole-corpus extract; a --journal
    restricted corpus would rebuild the baseline on the restricted set only.
    """
    edges = _edges_for_outer(data["config"], outer_pct)
    bins = data["config"]["z_bins"]

    new_papers = {}
    for doi, p in data["papers"].items():
        z = p.get("z")
        new_papers[doi] = {**p, "z_bin": _bin_of(z, edges) if z is not None else p.get("z_bin")}

    counts = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    for p in new_papers.values():
        if p.get("z_bin") is None:
            continue
        counts[p["journal_id"]][f'{p["type_bin"]}|{p["pages_bin"]}'][p["z_bin"]] += 1
    new_journals = {
        jid: {"baseline_counts": {sk: {b: binc.get(b, 0) for b in bins}
                                  for sk, binc in strata.items()}}
        for jid, strata in counts.items()
    }
    return {**data, "papers": new_papers, "journals": new_journals}


# ---------------------------------------------------------------------------
# Result breakdowns (thesis Tables: confidence tier + journals)
# ---------------------------------------------------------------------------

def tier_of(n_gaps):
    """Confidence tier by record length: low (n<4), medium (4-9), high (n>=10)."""
    if n_gaps < 4:
        return "low"
    return "medium" if n_gaps < 10 else "high"


def print_breakdowns(rows, shortlist, min_journal=5, cap=25):
    """Console tables for the thesis: shortlist rate per confidence tier, and the
    journals with the most shortlisted authors (an author counted once per journal
    they published in)."""
    tiers = ["low", "medium", "high"]
    ranges = {"low": "n<4", "medium": "4<=n<=9", "high": "n>=10"}
    tot = {t: 0 for t in tiers}
    short = {t: 0 for t in tiers}
    for r in rows:
        tot[tier_of(r["n_gaps"])] += 1
    for r in shortlist:
        short[tier_of(r["n_gaps"])] += 1

    print("\n=== SHORTLIST BY CONFIDENCE TIER (n_gaps: low<4, medium=4-9, high>=10) ===")
    print(f"  {'tier':<8}{'range':<10}{'authors':>9}{'shortlisted':>13}{'pct':>9}")
    for t in tiers:
        pct = 100 * short[t] / tot[t] if tot[t] else 0.0
        print(f"  {t:<8}{ranges[t]:<10}{tot[t]:>9}{short[t]:>13}{pct:>8.2f}%")
    gt, gs = sum(tot.values()), sum(short.values())
    print(f"  {'total':<8}{'':<10}{gt:>9}{gs:>13}{100*gs/max(gt,1):>8.2f}%")

    jcount = defaultdict(int)
    for r in shortlist:
        for jid in {g["journal_id"] for g in r["gaps"]}:
            jcount[jid] += 1
    ranked = sorted(jcount.items(), key=lambda kv: (-kv[1], str(kv[0])))
    print(f"\n=== JOURNALS BY #SHORTLISTED AUTHORS (>= {min_journal}; counted per journal) ===")
    print(f"  {'journal_id':<12}{'shortlisted':>12}")
    for jid, c in ranked[:cap]:
        if c < min_journal:
            break
        print(f"  {str(jid):<12}{c:>12}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="BBN V4 author-centric inference.")
    parser.add_argument("--in", dest="in_path", default=DEFAULT_IN, help="corpus JSON from extract")
    parser.add_argument("--threshold", type=float, default=SHORTLIST_THRESHOLD,
                        help="P(genuine) below which an author is shortlisted")
    parser.add_argument("--z-threshold", type=float, default=CANDIDATE_Z_THRESHOLD,
                        help="candidacy cut: an author is scored if >=1 gap has z below this")
    parser.add_argument("--selftest", action="store_true", help="run synthetic validation and exit")
    args = parser.parse_args()

    if args.selftest:
        return selftest()

    if not os.path.exists(args.in_path):
        raise SystemExit(f"Run bbn_extract_v3.py first; {args.in_path} not found.")
    with open(args.in_path, encoding="utf-8") as f:
        data = json.load(f)

    assert set(MANIP_DIST) == set(data["config"]["z_bins"]), "MANIP_DIST keys must match z_bins"

    rows, journals, pooled, bins = rank_corpus(data, ALPHA, PRIOR_GENUINE, z_threshold=args.z_threshold)
    shortlist = [r for r in rows if r["p_genuine"] < args.threshold]

    print(f"Model: {data['model']} | z_threshold={args.z_threshold} | alpha={ALPHA} | "
          f"prior P(genuine)={PRIOR_GENUINE} | MANIP_DIST={MANIP_DIST}")
    print(f"Scored {len(rows)} authors (>=1 gap with z < {args.z_threshold}); "
          f"{len(shortlist)} below threshold {args.threshold}.")

    # --- ranking CSV (all scored authors) ---------------------------------
    # log10_odds (weight of evidence) is the primary, non-saturating sort key;
    # p_genuine is written with full precision so tiny posteriors never read 0.
    rank_path = os.path.join(OUT_DIR, "bbn_v4_ranking.csv")
    cols = ["name", "orcid", "p_genuine", "log10_odds", "n_gaps", "n_nontypical", "n_journals",
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

    # --- results JSON (ALL scored authors, full per-gap evidence breakdown) --
    # Everything scored is persisted here, ordered most-suspicious first; the
    # P(genuine) < threshold cut only governs the terminal output below, not what
    # is written. Each author carries a `shortlisted` flag so the P<0.5 subset is
    # trivially recoverable, but nothing is dropped from the file.
    authors_out = []
    for r in rows:
        _, _, detail = score_author(r["gaps"], journals, pooled, bins, ALPHA, PRIOR_GENUINE, True)
        detail.sort(key=lambda d: d["lr"])  # most incriminating gap first
        authors_out.append({
            "name": r["name"], "orcid": r["orcid"], "p_genuine": r["p_genuine"],
            "log10_odds": round(r["log10_odds"], 3),
            "shortlisted": r["p_genuine"] < args.threshold,
            "n_gaps": r["n_gaps"], "n_nontypical": r["n_nontypical"],
            "n_journals": r["n_journals"],
            "gaps": detail,
        })
    json_path = os.path.join(OUT_DIR, "bbn_v4_scored.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump({"z_threshold": args.z_threshold, "alpha": ALPHA, "prior_genuine": PRIOR_GENUINE,
                   "shortlist_threshold": args.threshold, "manip_dist": MANIP_DIST,
                   "n_scored": len(authors_out),
                   "n_shortlisted": sum(1 for a in authors_out if a["shortlisted"]),
                   "authors": authors_out},
                  f, ensure_ascii=False, indent=2)
    print(f"Wrote {json_path}")

    # --- console: shortlist ranking + a sample evidence breakdown ---------
    print("\n=== INVESTIGATION PRIORITY (lowest genuineness first) ===")
    print("  (WoE = log10 posterior-odds of genuine; more negative = stronger evidence against."
          " P can underflow but WoE stays ordered. Absolute values are overconfident with large n"
          " -- the ranking + sensitivity are the reportable output.)")
    for r in shortlist:
        tag = f" {r['orcid']}" if r["orcid"] else ""
        print(f"  P(genuine)={fmt_p(r['p_genuine']):>9}  WoE={r['log10_odds']:>7.2f}  {r['name']:<28} "
              f"n_gaps={r['n_gaps']:<3} non-typ={r['n_nontypical']:<3} "
              f"j={r['n_journals']:<2} worst_z={r['lowest_z']:>7} [{r['lowest_bin']}]{tag}")

    if shortlist:
        top = authors_out[0]   # rows are sorted most-suspicious first, so this is the top shortlisted author
        print(f"\n=== EVIDENCE BREAKDOWN (lowest-scoring author: {top['name']!r}) ===")
        print("  (LR<1 lowers genuineness; a genuine author's gaps match the peer baseline)")
        for d in top["gaps"][:10]:
            print(f"     z={d['z']:>7} {d['z_bin']:<12} j={d['journal_id']:<7} "
                  f"genuine_p={d['genuine_p']:.4f} [{d['backoff_level']:<17}] LR={d['lr']:>7.3f}")
        if len(top["gaps"]) > 10:
            print(f"     ... (+{len(top['gaps']) - 10} more gaps)")

    # --- result breakdowns for the thesis tables --------------------------
    print_breakdowns(rows, shortlist)

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

        # --- m_b (manipulation-shape) sensitivity -------------------------
        # Vary only the shape of the elicited manipulation distribution. First the
        # P table for the top of the shortlist, then a ranking-stability summary
        # over the WHOLE shortlist (re-ranks the corpus per shape -- takes a bit).
        print(f"\n=== MANIP-SHAPE (m_b) SENSITIVITY  (P(genuine), alpha={ALPHA}, prior={PRIOR_GENUINE}; top {len(sweep)}) ===")
        print("  shape       " + "  ".join(f"{l:>11}" for l in labels))
        for sname, mdist in MANIP_SWEEP.items():
            line = f"  {sname:<11}"
            for r in sweep:
                p = score_author(r["gaps"], journals, pooled, bins, ALPHA, PRIOR_GENUINE, manip=mdist)[0]
                line += f"  {fmt_p(p):>11}"
            print(line)

        base_pos = {r["ident"]: i for i, r in enumerate(shortlist)}
        base_set = set(base_pos)
        print(f"\n=== m_b RANKING STABILITY vs baseline (baseline shortlist n={len(shortlist)}) ===")
        print("  (in-common = authors shortlisted under both; |Drank| = position shift within the shortlist order)")
        for sname, mdist in MANIP_SWEEP.items():
            rows_s, *_ = rank_corpus(data, ALPHA, PRIOR_GENUINE, manip=mdist, z_threshold=args.z_threshold)
            sl_s = [r for r in rows_s if r["p_genuine"] < args.threshold]
            pos_s = {r["ident"]: i for i, r in enumerate(sl_s)}
            common = base_set & set(pos_s)
            shifts = sorted(abs(base_pos[i] - pos_s[i]) for i in common)
            med = shifts[len(shifts) // 2] if shifts else 0
            print(f"  {sname:<11} size={len(sl_s):<5} in-common={len(common):>4}/{len(base_set):<4} "
                  f"median|Drank|={med:<4} max|Drank|={shifts[-1] if shifts else 0}")

        # --- z-edge (typical boundary) sensitivity ------------------------
        # Move ONLY the outer mild_fast/typical edge (p10..p25); the fast-tail
        # cuts p0.1/p1/p5 are held fixed. Candidacy is z<threshold on the
        # continuous z, so the candidate set does not change -- this isolates how
        # the exoneration boundary alone reshapes the ranking. Each point re-bins
        # every paper and rebuilds the genuine baseline (rebin_corpus), then
        # re-ranks. Requires the corpus to carry config.z_percentiles.
        pcts = data.get("config", {}).get("z_percentiles", {})
        needed = [px for px in Z_OUTER_PCT_SWEEP if f"p{px}" in pcts]
        if f"p{Z_OUTER_PCT_DEFAULT}" in pcts and len(needed) >= 2:
            reb_rows = {px: rank_corpus(rebin_corpus(data, px), ALPHA, PRIOR_GENUINE,
                                        z_threshold=args.z_threshold)[0] for px in needed}
            # faithfulness: rebuilt-default binning should reproduce the stored z_bin
            reb_def = rebin_corpus(data, Z_OUTER_PCT_DEFAULT)["papers"]
            drift = sum(1 for d, p in reb_def.items()
                        if p.get("z_bin") != data["papers"][d].get("z_bin"))
            print(f"\n=== Z-EDGE (typical boundary p_x) SENSITIVITY  (P(genuine), alpha={ALPHA}, "
                  f"prior={PRIOR_GENUINE}; top {len(sweep)}) ===")
            print(f"  (only the mild_fast/typical edge moves; candidate set fixed by z<{args.z_threshold}. "
                  f"rebuilt-p{Z_OUTER_PCT_DEFAULT} vs stored z_bin: {drift} papers differ.)")
            print("  p_x    " + "  ".join(f"{l:>11}" for l in labels))
            for px in needed:
                by_ident = {r["ident"]: r for r in reb_rows[px]}
                line = f"  p{px:<5}"
                for r in sweep:
                    rr = by_ident.get(r["ident"])
                    line += f"  {(fmt_p(rr['p_genuine']) if rr else '--'):>11}"
                print(line)

            ref_sl = [r for r in reb_rows[Z_OUTER_PCT_DEFAULT] if r["p_genuine"] < args.threshold]
            ref_pos = {r["ident"]: i for i, r in enumerate(ref_sl)}
            ref_set = set(ref_pos)
            print(f"\n=== Z-EDGE RANKING STABILITY vs p{Z_OUTER_PCT_DEFAULT}  "
                  f"(p{Z_OUTER_PCT_DEFAULT} shortlist n={len(ref_set)}) ===")
            print("  (in-common = authors shortlisted under both; |Drank| = position shift within the shortlist order)")
            for px in needed:
                sl_s = [r for r in reb_rows[px] if r["p_genuine"] < args.threshold]
                pos_s = {r["ident"]: i for i, r in enumerate(sl_s)}
                common = ref_set & set(pos_s)
                shifts = sorted(abs(ref_pos[i] - pos_s[i]) for i in common)
                med = shifts[len(shifts) // 2] if shifts else 0
                print(f"  p{px:<5} size={len(sl_s):<5} in-common={len(common):>4}/{len(ref_set):<4} "
                      f"median|Drank|={med:<4} max|Drank|={shifts[-1] if shifts else 0}")
        else:
            print("\n=== Z-EDGE SENSITIVITY skipped: corpus lacks config.z_percentiles "
                  f"for p{Z_OUTER_PCT_DEFAULT} and neighbours ===")


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
    # Pin an explicit operating point so the selftest validates the math, not the
    # module's current default ALPHA / MANIP_DIST (which are tuned for the chronic goal).
    A_T = 0.10
    M_T = {"typical": 0.0, "mild_fast": 0.15, "extreme": 0.20,
           "very_extreme": 0.25, "ultra_extreme": 0.40}
    P = lambda g, a=A_T, pr=PRIOR_GENUINE: score_author(g, journals, pooled, bins, a, pr, manip=M_T)[0]

    def gap(zb, z, doi, t="normal_type", p="normal"):
        return {"journal_id": "J1", "type_bin": t, "pages_bin": p, "z_bin": zb, "z": z, "doi": doi}

    A = [gap("very_extreme", -5.0, "A1")]
    C = [gap("typical", 0.2, "C1")]
    # z=-3.5 (not -3.0) so this gap is unambiguously past the CANDIDATE_Z_THRESHOLD
    # boundary and stays a V4 candidate; the label "extreme" here is just a
    # synthetic bin tag for exercising the back-off, unrelated to the real corpus's
    # percentile-derived bin edges.
    D = [gap("extreme", -3.5, "D1", t="fast_type", p="short")]
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
    lr0 = gap_lr("very_extreme", g0, A_T, M_T)
    odds0 = (PRIOR_GENUINE / (1 - PRIOR_GENUINE)) * lr0
    p_noloo = odds0 / (1 + odds0)
    check(f"no-exclusion posterior == 0.9209  (got {p_noloo:.6f})", _approx(p_noloo, 0.920895))
    check("leave-one-author LOWERS genuineness for the anomaly", pA < p_noloo)

    # 3. finer tail separates magnitude: an ultra_extreme gap is far more incriminating
    #    than a very_extreme one in the SAME stratum (the whole point of the p0.1 cut).
    g_ve, _ = genuine_dist(journals, pooled, bins, "J1", "normal_type", "normal", A)
    g_ue, _ = genuine_dist(journals, pooled, bins, "J1", "normal_type", "normal", E)
    lr_ve = gap_lr("very_extreme", g_ve, A_T, M_T)
    lr_ue = gap_lr("ultra_extreme", g_ue, A_T, M_T)
    check(f"LR_ultra ({lr_ue:.3f}) < LR_very_extreme ({lr_ve:.3f})", lr_ue < lr_ve)
    check(f"ultra_extreme author scored more suspicious than very_extreme  "
          f"(P {P(E):.3f} < {P(A):.3f})", P(E) < P(A))

    # 4. LR_typical == 1/(1-alpha) exactly, independent of counts/back-off
    gt, _ = genuine_dist(journals, pooled, bins, "J1", "normal_type", "normal", C)
    lrt = gap_lr("typical", gt, A_T, M_T)
    check(f"LR_typical == 1/(1-alpha)=1.11111  (got {lrt:.8f})", _approx(lrt, 1 / (1 - A_T), 1e-9))

    # 5. a typical-only author, and an author whose worst gap doesn't clear the
    #    z<-3 candidacy bar, are NOT candidates; those with a gap past it are.
    data = {"papers": {"A1": A[0], "C1": C[0], "D1": D[0], "E1": E[0]},
            "author_index": {"AuthA": ["A1"], "AuthC": ["C1"], "AuthD": ["D1"], "AuthE": ["E1"]}}
    cand = {ident for ident, _, _ in iter_candidates(data)}
    check("candidate set = {AuthA, AuthD, AuthE} (typical-only AuthC excluded)",
          cand == {"AuthA", "AuthD", "AuthE"})

    # 5b. an author whose single gap sits AT the boundary (z == threshold) is
    #     excluded: the cut is strictly "<", matching Westerbaan's "<-3" wording.
    boundary_data = {"papers": {"B1": gap("very_extreme", CANDIDATE_Z_THRESHOLD, "B1")},
                     "author_index": {"AuthB": ["B1"]}}
    cand_boundary = {ident for ident, _, _ in iter_candidates(boundary_data)}
    check("author with z exactly at the threshold is NOT a candidate (strict <)",
          cand_boundary == set())

    # ORCID identity -> display label resolved, orcid column populated
    odata = {"config": {"z_bins": bins}, "journals": journals,
             "papers": {"P1": A[0]},
             "author_index": {"0000-0001-2345-6789": ["P1"]},
             "author_labels": {"0000-0001-2345-6789": "Jane Doe"}}
    orows = rank_corpus(odata)[0]
    check("ORCID identity -> name=label and orcid column set",
          orows[0]["name"] == "Jane Doe" and orows[0]["orcid"] == "0000-0001-2345-6789")

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

    # 8. re-binning for the z-edge sweep. A tiny corpus with known percentiles and
    #    one paper (z=-0.7) parked between p15 and p25: it must be `typical` under
    #    the p15 edge and `mild_fast` under a tighter p25 edge, and the rebuilt
    #    baseline must move exactly one count between those bins.
    edge_cfg = {"z_bins": bins,
                "z_percentiles": {"p0.1": -6.0, "p1": -2.9, "p5": -1.6, "p10": -1.1,
                                  "p15": -0.9, "p20": -0.6, "p25": -0.4, "p50": 0.0}}
    edge_papers = {
        "Q1": {"journal_id": "J9", "z": -0.70, "z_bin": "typical",
               "type_bin": "normal_type", "pages_bin": "normal"},
        "Q2": {"journal_id": "J9", "z": -4.00, "z_bin": "very_extreme",
               "type_bin": "normal_type", "pages_bin": "normal"},
    }
    edge_data = {"config": edge_cfg, "papers": edge_papers,
                 "author_index": {"AuthQ": ["Q1", "Q2"]}}
    reb15 = rebin_corpus(edge_data, 15)
    reb25 = rebin_corpus(edge_data, 25)
    check("z=-0.7 is `typical` under the p15 outer edge",
          reb15["papers"]["Q1"]["z_bin"] == "typical")
    check("z=-0.7 becomes `mild_fast` under the tighter p25 outer edge",
          reb25["papers"]["Q1"]["z_bin"] == "mild_fast")
    check("z=-4.0 stays `very_extreme` (inner cuts fixed) under both edges",
          reb15["papers"]["Q2"]["z_bin"] == "very_extreme"
          and reb25["papers"]["Q2"]["z_bin"] == "very_extreme")
    c15 = reb15["journals"]["J9"]["baseline_counts"]["normal_type|normal"]
    c25 = reb25["journals"]["J9"]["baseline_counts"]["normal_type|normal"]
    check("rebuilt baseline moves exactly one count typical->mild_fast at p25",
          c15["typical"] == 1 and c15["mild_fast"] == 0
          and c25["typical"] == 0 and c25["mild_fast"] == 1)
    check("candidate set is edge-invariant (z<-3 candidacy is on continuous z)",
          {i for i, _, _ in iter_candidates(reb15)} == {i for i, _, _ in iter_candidates(reb25)})

    print("\nSELFTEST:", "ALL PASS" if ok else "FAILURES PRESENT")
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()