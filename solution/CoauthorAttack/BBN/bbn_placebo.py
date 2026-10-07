"""
Within-paper placebo tests: every statistic of the editor on a suspect paper is
recomputed with each non-board signer of that paper in the editor's place.

    1. venue-locking: the share of the focal person's co-authors on the paper
       with whom they published only inside this journal.
    2. pattern fit: where the focal person sits in each co-author's own
       collaborator distribution. Needs bbn/signer_pub_authors.csv.

Writes bbn/bbn_cs2_placebo.csv with the per-editor permutation test.

    python bbn_placebo.py [--in bbn]
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
import statistics
from collections import defaultdict
from pathlib import Path
from typing import NamedTuple

from bbn_extract import (BoardIndex, band_of, collaboration_networks, is_inside,
                         midrank_pct, person_id, publications_of)
from cs2_config import (BBN_DIR, FIT_MIN_COLLABS, NETWORK_BANDS, PLACEBO_RECORD_BANDS,
                        PLACEBO_SAMPLES, PLACEBO_SEED, REPORTS_DIR)
from dblp_extract import as_bool, load_mapping, pct, read_csv

TOP_N = 12      # console only: the editors listed under the permutation test


class Pool(NamedTuple):
    """One suspect paper x editor. `signers[0]` is the editor, the rest are non-board signers."""
    editor: str
    rec_key: str
    journal: str
    inside_keys: set
    signers: list


def suspect_pools(in_dir, mapping, roster, articles, signatures) -> list[Pool]:
    """A Pool for every (non-front-matter suspect paper x editor) with enough signers.

    `roster` maps a journal to the BoardIndex of every name on its board.
    """
    sigs = defaultdict(list)
    for s in signatures:
        sigs[s["rec_key"]].append(s)
    suspects = defaultdict(set)
    for p in read_csv(in_dir / "suspect_papers.csv"):
        if not as_bool(p.get("front_matter")):
            suspects[p["rec_key"]].add(p["editor_id"])

    pools, skipped = [], 0
    for rec_key, editors in suspects.items():
        article = articles.get(rec_key)
        if not article:
            skipped += 1
            continue
        journal = article["journal"]
        keys = set(mapping.get(journal, []))
        if not keys:
            skipped += 1
            continue
        people = list({person_id(s["pid"], s["name"]): s
                       for s in sigs.get(rec_key, [])}.values())
        board_here = roster.get(journal, BoardIndex())
        outsiders = [person_id(s["pid"], s["name"]) for s in people
                     if board_here.match(s["name"]) is None]
        if len(outsiders) < 2:
            continue
        for editor in sorted(editors):
            signers = [editor] + [i for i in outsiders if i != editor]
            if len(signers) >= 3:
                pools.append(Pool(editor, rec_key, journal, keys, signers))
    if skipped:
        print(f"  ({skipped} suspect papers skipped: no article or no journal key)")
    return pools


# ---------------------------------------------------------------------------
# Test 1: venue-locking, from the editor's side
# ---------------------------------------------------------------------------

def venue_locked(a: str, b: str, inside_keys, this_paper: str, pubs_of) -> int:
    """1 if everything a and b published together, this paper included, is inside this journal."""
    shared = (pubs_of.get(a, set()) & pubs_of.get(b, set())) | {this_paper}
    return 0 if any(not is_inside(r, inside_keys) for r in shared) else 1


def locked_pairs(signers, inside_keys, rec_key, pubs_of) -> dict:
    """venue_locked for every unordered pair of signers, keyed by (i, j) with i < j."""
    return {(x, y): venue_locked(signers[x], signers[y], inside_keys, rec_key, pubs_of)
            for x in range(len(signers)) for y in range(x + 1, len(signers))}


def locked_fraction(lock, i, n) -> float:
    """Share of the other n-1 signers that signer i is venue-locked with."""
    return statistics.fmean([lock[tuple(sorted((i, k)))]
                             for k in range(n) if k != i])


def venue_units(pools: list[Pool], pubs_of) -> list[dict]:
    """Per pool: the editor's venue-locked fraction beside the fraction each
    non-board stand-in would have had in their place."""
    units = []
    for pool in pools:
        n = len(pool.signers)
        lock = locked_pairs(pool.signers, pool.inside_keys, pool.rec_key, pubs_of)
        fraction = [locked_fraction(lock, i, n) for i in range(n)]
        sizes = [len(pubs_of.get(p, ())) for p in pool.signers]
        stand_ins = range(1, n)
        matched = [i for i in stand_ins
                   if band_of(sizes[i], PLACEBO_RECORD_BANDS)
                   == band_of(sizes[0], PLACEBO_RECORD_BANDS)]
        units.append({
            "editor": pool.editor,
            "observed": fraction[0],
            "alternatives": [fraction[i] for i in stand_ins],
            "senior": fraction[max(stand_ins, key=lambda i: sizes[i])],
            "matched": (statistics.fmean([fraction[i] for i in matched])
                        if matched else None),
            "editor_is_most_prolific": sizes[0] >= max(sizes),
        })
    return units


# ---------------------------------------------------------------------------
# Test 2: pattern fit, from the co-author's side
# ---------------------------------------------------------------------------

PATTERN_METRICS = ["pct_depth", "pct_spread", "is_min_depth", "lone_venue"]
PATTERN_LABEL = {
    "pct_depth": "percentile by shared publications",
    "pct_spread": "percentile by shared venues",
    "is_min_depth": "sits at the partner's minimum depth",
    "lone_venue": "shares only one venue with the partner",
}
# Low percentiles, and high min-depth / lone-venue rates.
PATTERN_SIGN = {"pct_depth": "NEGATIVE", "pct_spread": "NEGATIVE",
                "is_min_depth": "POSITIVE", "lone_venue": "POSITIVE"}


def describe(depth, spread) -> dict:
    """Each person's own collaboration pattern, as a distribution to rank into."""
    out = {}
    for a, cols in depth.items():
        if len(cols) < FIT_MIN_COLLABS:
            continue
        d = list(cols.values())
        out[a] = {"depth": d, "spread": [len(spread[a][c]) for c in cols],
                  "min_depth": min(d), "n": len(cols)}
    return out


def signer_pattern(signers, i, depth, spread, pattern):
    """Where signer i sits in each OTHER signer's own collaborator distribution,
    averaged over the partners that have one. None when none of them does."""
    focal = signers[i]
    acc = {m: [] for m in PATTERN_METRICS}
    for k, partner in enumerate(signers):
        pat = pattern.get(partner)
        if k == i or not pat or focal not in depth[partner]:
            continue
        d = depth[partner][focal]
        s = len(spread[partner][focal])
        acc["pct_depth"].append(midrank_pct(pat["depth"], d))
        acc["pct_spread"].append(midrank_pct(pat["spread"], s))
        acc["is_min_depth"].append(1.0 if d == pat["min_depth"] else 0.0)
        acc["lone_venue"].append(1.0 if s == 1 else 0.0)
    if not acc["pct_depth"]:
        return None
    return {m: statistics.fmean(v) for m, v in acc.items()}


def pattern_units(pools: list[Pool], depth, spread, pattern) -> list[dict]:
    """Per pool: the editor's pattern metrics beside their stand-ins'."""
    units = []
    for pool in pools:
        signers = pool.signers
        observed = signer_pattern(signers, 0, depth, spread, pattern)
        alts = [(i, signer_pattern(signers, i, depth, spread, pattern))
                for i in range(1, len(signers))]
        alts = [(i, v) for i, v in alts if v is not None]
        if observed is None or not alts:
            continue
        ns = [pattern[q]["n"] if q in pattern else 0 for q in signers]
        same = [v for i, v in alts
                if band_of(ns[i], NETWORK_BANDS) == band_of(ns[0], NETWORK_BANDS)]
        units.append({
            "editor": pool.editor, "observed": observed,
            "alternatives": {m: statistics.fmean([v[m] for _, v in alts])
                             for m in PATTERN_METRICS},
            "matched": ({m: statistics.fmean([v[m] for v in same])
                         for m in PATTERN_METRICS} if same else None),
        })
    return units


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

def cluster_by_editor(editors, observed, comparator):
    """Per-editor mean paired difference, over the units where both values exist."""
    per_editor = defaultdict(list)
    kept = []
    for e, o, c in zip(editors, observed, comparator):
        if c is None:
            continue
        per_editor[e].append(o - c)
        kept.append((o, c))
    return kept, [statistics.fmean(d) for d in per_editor.values()]


def exact_test(units, samples=PLACEBO_SAMPLES) -> list[dict]:
    """Per editor: a Monte-Carlo permutation p-value over their own papers' stand-ins.

    Each editor has their own generator, seeded from PLACEBO_SEED and their id.
    """
    by_editor = defaultdict(list)
    for u in units:
        by_editor[u["editor"]].append([u["observed"]] + u["alternatives"])
    results = []
    for editor, choices in by_editor.items():
        rng = random.Random(f"{PLACEBO_SEED}:{editor}")
        observed = sum(c[0] for c in choices)
        expected = sum(statistics.fmean(c) for c in choices)
        at_least = sum(1 for _ in range(samples)
                       if sum(rng.choice(c) for c in choices) >= observed - 1e-9)
        results.append({"editor": editor, "n_papers": len(choices),
                        "observed": observed, "expected": expected,
                        "p": (at_least + 1) / (samples + 1)})
    results.sort(key=lambda r: (r["p"], -(r["observed"] - r["expected"])))
    return results


def bh_q(ps) -> list[float]:
    """Benjamini-Hochberg adjusted p-values, for p-values sorted ascending."""
    n = len(ps)
    q, running = [0.0] * n, 1.0
    for i in range(n - 1, -1, -1):
        running = min(running, ps[i] * n / (i + 1))
        q[i] = running
    return q


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def paired(label, editors, observed, comparator) -> None:
    """Paired difference: unit-level means, and diff and t over the per-editor means."""
    kept, editor_diffs = cluster_by_editor(editors, observed, comparator)
    if len(editor_diffs) < 2:
        print(f"    {label:<36} (too few editors)")
        return
    mean = statistics.fmean(editor_diffs)
    se = statistics.stdev(editor_diffs) / math.sqrt(len(editor_diffs))
    print(f"    {label:<36}{len(kept):>7}{len(editor_diffs):>8}"
          f"{statistics.fmean([p[0] for p in kept]):>10.4f}"
          f"{statistics.fmean([p[1] for p in kept]):>10.4f}"
          f"{mean:>+10.4f}{(mean / se if se else float('nan')):>+9.2f}")


def header() -> None:
    print(f"    {'comparator':<36}{'units':>7}{'editors':>8}{'editor':>10}"
          f"{'others':>10}{'diff':>10}{'t':>9}")


def report_venue_locking(units) -> None:
    observed = [u["observed"] for u in units]
    editors = [u["editor"] for u in units]
    print("\n  1. VENUE-LOCKED CO-AUTHOR FRACTION, from the editor's side")
    print("     (the attack predicts a POSITIVE difference)")
    header()
    paired("all non-board signers", editors, observed,
           [statistics.fmean(u["alternatives"]) for u in units])
    paired("the most prolific non-board signer", editors, observed,
           [u["senior"] for u in units])
    paired("non-board signers, same record band", editors, observed,
           [u["matched"] for u in units])


def report_pattern_fit(pools, net_path) -> None:
    wanted = {p for pool in pools for p in pool.signers}
    depth, spread = collaboration_networks(net_path, wanted, with_venues=True)
    pattern = describe(depth, spread)
    units = pattern_units(pools, depth, spread, pattern)
    editors = [u["editor"] for u in units]
    print(f"\n  2. PATTERN FIT, from the co-author's side "
          f"({len(units)} units, {len(pattern)} partner patterns)")
    print("     where does the focal person sit in their partner's OWN "
          "collaborator distribution?")
    for m in PATTERN_METRICS:
        print(f"\n     {PATTERN_LABEL[m]}  "
              f"(the attack predicts a {PATTERN_SIGN[m]} difference)")
        header()
        paired("all non-board signers", editors,
               [u["observed"][m] for u in units],
               [u["alternatives"][m] for u in units])
        paired("stand-ins of similar network size", editors,
               [u["observed"][m] for u in units],
               [u["matched"][m] if u["matched"] else None for u in units])


def report_exact_test(results, labels, samples) -> None:
    n = len(results)
    print(f"\n  exact permutation null over {n} editors "
          f"({samples} draws each), on the venue-locked statistic:")
    ps = sorted(r["p"] for r in results)
    for q in (0.01, 0.05, 0.10, 0.25):
        print(f"    P(p <= {q:<5}) = {sum(1 for x in ps if x <= q) / n:>7.4f}"
              f"   (expected {q})")
    qs = bh_q(ps)
    print(f"    editors surviving Benjamini-Hochberg q <= 0.25: "
          f"{sum(1 for q in qs if q <= 0.25)}")

    print(f"\n  {'p':>9}{'BH q':>8}{'papers':>8}{'obs':>8}{'exp':>8}  editor")
    for r, q in zip(results[:TOP_N], qs):
        print(f"  {r['p']:>9.5f}{q:>8.3f}"
              f"{r['n_papers']:>8}{r['observed']:>8.2f}{r['expected']:>8.2f}  "
              f"{labels.get(r['editor'], r['editor'])[:34]}")


def write_results_csv(path, results, labels) -> None:
    qs = bh_q([r["p"] for r in results])
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["name", "n_papers", "observed",
                                               "expected", "p", "bh_q"])
        writer.writeheader()
        for r, q in zip(results, qs):
            writer.writerow({"name": labels.get(r["editor"], r["editor"]),
                             "n_papers": r["n_papers"],
                             "observed": round(r["observed"], 4),
                             "expected": round(r["expected"], 4),
                             "p": round(r["p"], 6),
                             "bh_q": round(q, 4)})


def editor_labels(corpus_path) -> dict:
    """Printable editor names from the corpus, or {} when it has not been built."""
    if not corpus_path.exists():
        return {}
    return json.loads(corpus_path.read_text(encoding="utf-8")).get("editor_labels", {})


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="CS2 within-paper placebo tests.")
    parser.add_argument("--in", dest="in_dir", default=str(BBN_DIR),
                        help="directory holding the pipeline's CSVs (default: bbn)")
    in_dir = Path(parser.parse_args().in_dir)

    mapping = load_mapping(REPORTS_DIR / "journal_mapping.csv")
    articles = {a["rec_key"]: a for a in
                read_csv(REPORTS_DIR / "extract" / "articles.csv")}
    signatures = read_csv(REPORTS_DIR / "extract" / "signatures.csv")
    pubs_of = publications_of(in_dir / "pid_publications.csv")
    roster = defaultdict(set)
    for r in read_csv(in_dir / "board_roster.csv"):
        roster[r["journal"]].add(r["name"])
    roster = {journal: BoardIndex(names) for journal, names in roster.items()}

    print("=== WITHIN-PAPER PLACEBO ===")
    pools = suspect_pools(in_dir, mapping, roster, articles, signatures)
    units = venue_units(pools, pubs_of)
    prolific = sum(1 for u in units if u["editor_is_most_prolific"])
    print(f"  units (suspect paper x editor) {len(units)}")
    print(f"  the editor is the most prolific signer on {prolific} of them "
          f"({pct(prolific, len(units))})")

    report_venue_locking(units)

    net_path = in_dir / "signer_pub_authors.csv"
    if net_path.exists():
        report_pattern_fit(pools, net_path)
    else:
        print("\n  2. PATTERN FIT: unavailable -- run "
              "`python bbn_extract.py --stage signer_authors` first.")

    results = exact_test(units)
    labels = editor_labels(in_dir / "bbn_cs2_corpus.json")
    report_exact_test(results, labels, PLACEBO_SAMPLES)

    out_path = in_dir / "bbn_cs2_placebo.csv"
    write_results_csv(out_path, results, labels)
    print(f"\nWrote {out_path}")


if __name__ == "__main__":
    main()
