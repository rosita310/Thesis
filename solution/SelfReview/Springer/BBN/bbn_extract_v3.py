"""
BBN V3 corpus extraction for the self-review case study (SQ1.1).

Same per-gap "plate" model as V2 (latent G = is_genuine; one gap node per gap,
parents (G, article_type, pages); gaps are the conditionally-independent unit of
evidence). What changed in V3:

  * AUTHOR-CENTRIC, CORPUS-WIDE OUTPUT. No longer bound to 3 hardcoded suspects.
    We emit, once and deduplicated, everything the inference needs to score ANY
    author:
        journals      -- per-journal genuine baseline counts P(z-bin | type, pages)
        papers        -- one entry per usable DOI (journal, gap, z, z_bin, bins)
        author_index  -- identity -> [doi, ...]  (cross-journal)
        author_labels -- identity -> representative display name
        
    Author identity is the hybrid ORCID-or-name key (Punt 4): a row's ORCID if
    springer.author_orcid has one, propagated to that author's ORCID-less rows
    when the name maps to exactly one ORCID, else the name string. Falls back to
    name-only identities if springer.author_orcid is absent.

  * DATA-DERIVED Z-EDGES. The fixed z = -1/-2/-4 cuts are
    replaced by the pooled percentiles of the standardized log-gap z over all
    journals:
        ultra_extreme <= p0.1 < very_extreme <= p1 < extreme <= p5
        < mild_fast <= p15 < typical.
    The extra p0.1 cut keeps tail magnitude (a z=-9 corpus outlier is not lumped
    with a z~-2.9 p1 gap). The edges actually used are written to config.z_edges
    so `infer` stays in sync automatically (it reads the precomputed z_bin).

  * NO baseline exclusion here. The baseline counts include every usable paper;
    leave-one-AUTHOR exclusion is applied at inference time (so each author is
    judged against peers, not themselves) -- see bbn_infer_v3.py.

z transform & per-journal reference are unchanged from V1/V2:
  transform = ln(max(gap_days, GAP_FLOOR_DAYS)); per-journal sample mean/std,
  high outliers (> HIGH_GAP_CUTOFF_DAYS) trimmed from the reference.

Run from this directory (BBN/) with the scraper venv (DB via pyodbc):
    python bbn_extract_v3.py                 # whole corpus
    python bbn_extract_v3.py --journal 10623 # restrict to one journal's authors
"""

from __future__ import annotations

import argparse
import configparser
import json
import math
import os
import statistics
import sys
from collections import Counter, defaultdict


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SCHEMA = "springer"

HIGH_GAP_CUTOFF_DAYS = 924
GAP_FLOOR_DAYS = 0.5
MIN_JOURNAL_REF = 2             # journal needs >=2 usable gaps (and std>0) to define z

# Vooraf B: z-bin edges are the pooled percentiles of the standardized log-gap z.
# Edges are upper z-thresholds, fastest first. Refined on the fast (left) tail so
# the model has resolution where manipulation lives; the slow side stays coarse
# (manipulation never makes a review slower). The deep tail is split with an extra
# p0.1 cut (`ultra_extreme`) so a corpus-level outlier (e.g. z=-9) is no longer
# lumped with a merely-p1 gap (z~-2.9) -- the single coarse bin discarded magnitude.
Z_BINS = ["typical", "mild_fast", "extreme", "very_extreme", "ultra_extreme"]
PCT_EDGES = [(0.1, "ultra_extreme"), (1, "very_extreme"), (5, "extreme"), (15, "mild_fast")]


def percentile(sorted_vals, pct):
    """Linear-interpolated percentile (pct in [0,100]) on an ascending list."""
    n = len(sorted_vals)
    if n == 1:
        return sorted_vals[0]
    k = (n - 1) * (pct / 100.0)
    lo, hi = math.floor(k), math.ceil(k)
    if lo == hi:
        return sorted_vals[int(k)]
    return sorted_vals[lo] * (hi - k) + sorted_vals[hi] * (k - lo)


def build_z_edges(sorted_z):
    """Derive [(z_threshold, label), ..., (inf, 'typical')] from pooled percentiles."""
    edges = [(percentile(sorted_z, pct), label) for pct, label in PCT_EDGES]
    edges.append((float("inf"), "typical"))
    return edges


def make_bin_z(z_edges):
    def bin_z(z):
        for edge, label in z_edges:
            if z <= edge:
                return label
        return Z_BINS[0]
    return bin_z


# per-gap context bins (parents of each gap node)
FAST_TYPE_KEYWORDS = (
    "editorial", "erratum", "correction", "corrigendum", "comment",
    "letter", "preface", "introduction", "book review", "obituary",
    "addendum", "retraction", "foreword", "in memoriam",
)
SHORT_PAGES_MAX = 4

ENV_PATH = "../../../.env"      # run from BBN/ (matches the V1/V2 scripts)
OUT_DIR = os.path.join(os.path.dirname(__file__), "bbn_baselines")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def read_config(path) -> configparser.SectionProxy:
    if not os.path.exists(path):
        sys.exit(f"\nERROR: no .env found at {os.path.abspath(path)}\n"
                 f"Copy code/env-example to code/.env and fill in the POSTGRES_* values.\n")
    with open(path, "r") as f:
        config_string = "[SECTION]\n" + f.read()
    config = configparser.ConfigParser()
    config.read_string(config_string)
    return config["SECTION"]


def pages_of(first, last):
    if first is None or last is None or last < first:
        return None
    return last - first + 1


def type_bin(article_type):
    t = (article_type or "").lower()
    return "fast_type" if any(k in t for k in FAST_TYPE_KEYWORDS) else "normal_type"


def pages_bin(pages):
    if pages is None:
        return "unknown"
    return "short" if pages <= SHORT_PAGES_MAX else "normal"


def stratum_key(tbin, pbin):
    return f"{tbin}|{pbin}"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="BBN V3 corpus extraction.")
    parser.add_argument("--journal", default=None,
                        help="restrict the emitted papers/author_index to one journal_id "
                             "(the genuine baselines are always corpus-wide).")
    args = parser.parse_args()

    # DB import kept inside main() so the pure helpers stay importable without pyodbc.
    from database import Postgress

    config = read_config(ENV_PATH)
    db = Postgress(
        server=config["POSTGRES_SERVER"],
        database=config["POSTGRES_DB"],
        user=config["POSTGRES_USER"],
        password=config["POSTGRES_PASSWORD"],
    )

    print("Loading all journals ...")
    articles = db.execute_query_result(f"""
        SELECT doi, journal_id, received, review_days, article_type, first_page, last_page
        FROM "{SCHEMA}"."articles"
    """)
    author_rows = db.execute_query_result(f"""
        SELECT doi, name, journal_id
        FROM "{SCHEMA}"."authors"
    """)

    name_to_dois = defaultdict(set)
    for r in author_rows:
        name_to_dois[r["name"]].add(r["doi"])

    # --- ORCID identity (Punt 4): hybrid key = ORCID where known, else name -----
    # Load the DBLP->springer ORCID map (springer.author_orcid) if present. A row's
    # identity = its own ORCID; if it has none but its name maps to exactly ONE
    # ORCID corpus-wide, propagate that ORCID (consolidate the person's record);
    # otherwise fall back to the name string.
    row_orcid, name_orcids = {}, defaultdict(set)
    if db.table_exists(SCHEMA, "author_orcid"):
        for r in db.execute_query_result(
                f'SELECT doi, springer_name, orcid FROM "{SCHEMA}"."author_orcid" '
                f"WHERE orcid IS NOT NULL AND orcid <> ''"):
            row_orcid[(r["doi"], r["springer_name"])] = r["orcid"]
            name_orcids[r["springer_name"]].add(r["orcid"])
    name_unique_orcid = {n: next(iter(s)) for n, s in name_orcids.items() if len(s) == 1}
    all_orcids = {o for s in name_orcids.values() for o in s}

    def identity_of(doi, name):
        return row_orcid.get((doi, name)) or name_unique_orcid.get(name) or name

    # --- pass 1: per-paper transform + per-journal reference ----------------
    papers = {}
    journal_vals = defaultdict(list)
    neg_gaps = zero_gaps = 0
    for a in articles:
        gap = a["review_days"]
        if gap is None or gap < 0:
            neg_gaps += (gap is not None and gap < 0)
            continue
        if gap == 0:
            zero_gaps += 1
        if gap > HIGH_GAP_CUTOFF_DAYS:
            continue
        t = math.log(max(gap, GAP_FLOOR_DAYS))
        papers[a["doi"]] = {
            "doi": a["doi"], "journal_id": a["journal_id"], "gap": gap, "t": t,
            "article_type": a["article_type"], "type_bin": type_bin(a["article_type"]),
            "pages": pages_of(a["first_page"], a["last_page"]),
        }
        journal_vals[a["journal_id"]].append(t)

    journal_ref = {}
    for jid, vals in journal_vals.items():
        if len(vals) < MIN_JOURNAL_REF:
            continue
        sd = statistics.stdev(vals)
        if sd > 0:
            journal_ref[jid] = (statistics.fmean(vals), sd)

    for p in papers.values():
        ref = journal_ref.get(p["journal_id"])
        p["z"] = (p["t"] - ref[0]) / ref[1] if ref else None
        p["pages_bin"] = pages_bin(p["pages"])

    # --- derive z-edges from pooled standardized z ----------------
    sorted_z = sorted(p["z"] for p in papers.values() if p["z"] is not None)
    if not sorted_z:
        raise SystemExit("No usable z values; cannot derive bin edges.")
    z_edges = build_z_edges(sorted_z)
    bin_z = make_bin_z(z_edges)

    percentiles = {f"p{p}": round(percentile(sorted_z, p), 4)
                   for p in (0.1, 0.5, 1, 2, 5, 10, 15, 25, 50)}

    for p in papers.values():
        p["z_bin"] = bin_z(p["z"]) if p["z"] is not None else None

    usable = [p for p in papers.values() if p["z_bin"] is not None]
    print(f"Usable gaps: {len(usable)} (0-day floored: {zero_gaps}; negative excluded: {neg_gaps}); "
          f"journals with z reference: {len(journal_ref)}")
    print("Pooled z percentiles: " + ", ".join(f"{k}={v:+.2f}" for k, v in percentiles.items()))
    print("z-edges in use:       "
          + ", ".join(f"{lbl}<=({e:+.3f})" if e != float('inf') else f"{lbl}=rest"
                      for e, lbl in z_edges))
    pooled_bins = defaultdict(int)
    for p in usable:
        pooled_bins[p["z_bin"]] += 1
    print("Pooled bin shares:    "
          + ", ".join(f"{b}={pooled_bins[b]/len(usable):.4f}" for b in Z_BINS))

    # --- JOURNAL-SPECIFIC genuine baseline counts P(z-bin | type, pages) -----
    # No exclusion here: every usable paper counts. Leave-one-author exclusion is
    # applied at inference time. counts[journal_id][stratum_key][z_bin].
    counts = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    for p in usable:
        counts[p["journal_id"]][stratum_key(p["type_bin"], p["pages_bin"])][p["z_bin"]] += 1

    journals_out = {}
    for jid, ref in journal_ref.items():
        baseline = {sk: {b: bins.get(b, 0) for b in Z_BINS}
                    for sk, bins in counts.get(jid, {}).items()}
        journals_out[jid] = {
            "ref_mean": ref[0], "ref_std": ref[1],
            "n_papers": sum(sum(b.values()) for b in baseline.values()),
            "baseline_counts": baseline,
        }

    # --- papers + author_index (optionally restricted to one journal) --------
    if args.journal is not None:
        keep_dois = {p["doi"] for p in usable if p["journal_id"] == args.journal}
    else:
        keep_dois = {p["doi"] for p in usable}

    papers_out = {}
    for d in keep_dois:
        p = papers[d]
        papers_out[d] = {
            "journal_id": p["journal_id"], "gap_days": p["gap"],
            "z": round(p["z"], 3), "z_bin": p["z_bin"],
            "type_bin": p["type_bin"], "pages_bin": p["pages_bin"],
            "article_type": p["article_type"],
        }

    # group authorships into identities (ORCID-or-name); label = most common name
    ident_dois = defaultdict(set)
    ident_names = defaultdict(Counter)
    for r in author_rows:
        if r["doi"] not in papers_out:
            continue
        ident = identity_of(r["doi"], r["name"])
        ident_dois[ident].add(r["doi"])
        ident_names[ident][r["name"]] += 1
    author_index = {i: sorted(dois) for i, dois in ident_dois.items()}
    author_labels = {i: cnt.most_common(1)[0][0] for i, cnt in ident_names.items()}
    n_orcid_ident = sum(1 for i in author_index if i in all_orcids)

    # --- report ------------------------------------------------------------
    print(f"\nCorpus: {len(papers_out)} usable papers, {len(author_index)} author identities "
          f"with >=1 usable paper, {len(journals_out)} journal baselines."
          + (f"  (restricted to journal {args.journal})" if args.journal else ""))
    if all_orcids:
        print(f"ORCID grouping: {n_orcid_ident}/{len(author_index)} identities ORCID-keyed "
              f"({n_orcid_ident/max(len(author_index),1):.1%}); "
              f"{len(name_unique_orcid)} names had a unique ORCID for propagation.")
    else:
        print("ORCID grouping: springer.author_orcid not found -> name-only identities.")

    os.makedirs(OUT_DIR, exist_ok=True)
    out_path = os.path.join(
        OUT_DIR, f"bbn_v3_corpus{('_' + args.journal) if args.journal else ''}.json")
    out = {
        "model": "v3_per_gap_corpus",
        "journal_filter": args.journal,
        "config": {
            "high_gap_cutoff_days": HIGH_GAP_CUTOFF_DAYS, "gap_floor_days": GAP_FLOOR_DAYS,
            "min_journal_ref": MIN_JOURNAL_REF,
            "z_edges": [[e if e != float("inf") else "inf", lbl] for e, lbl in z_edges],
            "z_bins": Z_BINS,
            "z_percentiles": percentiles,
            "short_pages_max": SHORT_PAGES_MAX,
        },
        "journals": journals_out,
        "papers": papers_out,
        "author_index": author_index,
        "author_labels": author_labels,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2, default=str)
    print(f"\nWrote {out_path}")


if __name__ == "__main__":
    main()
