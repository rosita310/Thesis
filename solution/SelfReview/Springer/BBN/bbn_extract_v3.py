"""
BBN V3 corpus extraction for the self-review case study (SQ1.1).

Same per-gap "plate" model as V2 (latent G = is_genuine; one gap node per gap,
parents (G, article_type, pages); gaps are the conditionally-independent unit of
evidence). What changed in V3:

  * AUTHOR-CENTRIC, CORPUS-WIDE OUTPUT. No longer bound to 3 hardcoded suspects.
    We emit, once and deduplicated, everything the inference needs to score ANY
    author:
        journals     -- per-journal genuine baseline counts P(z-bin | type, pages)
        papers       -- one entry per usable DOI (journal, gap, z, z_bin, bins)
        author_index -- name -> [doi, ...]  (cross-journal)
        suspects     -- the original 3 names, kept only as a validation anchor.

  * DATA-DERIVED Z-EDGES (stappenplan "Vooraf B"). The fixed z = -1/-2/-4 cuts are
    replaced by the pooled percentiles of the standardized log-gap z over all
    journals: very_extreme <= p1 < extreme <= p5 < mild_fast <= p15 < typical.
    The edges actually used are written to config.z_edges so `infer` stays in
    sync automatically (it reads the precomputed z_bin, never re-bins).

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
from collections import defaultdict


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SCHEMA = "springer"

# Original case-study co-authors. No longer drive extraction; kept as a labeled
# subset so downstream validation can find them quickly.
SUSPECTS = {
    "R. Radheshwar",
    "Dibyendu Roy",
    "Pantelimon Stănică",
}

HIGH_GAP_CUTOFF_DAYS = 924
GAP_FLOOR_DAYS = 0.5
MIN_JOURNAL_REF = 2             # journal needs >=2 usable gaps (and std>0) to define z

# Vooraf B: z-bin edges are the pooled percentiles of the standardized log-gap z.
# Edges are upper z-thresholds, fastest first. Refined on the fast (left) tail so
# the model has resolution where manipulation lives; the slow side stays coarse
# (manipulation never makes a review slower).
Z_BINS = ["typical", "mild_fast", "extreme", "very_extreme"]
PCT_EDGES = [(1, "very_extreme"), (5, "extreme"), (15, "mild_fast")]  # percentile -> bin


def build_z_edges(pooled_z):
    """Derive [(z_threshold, label), ..., (inf, 'typical')] from pooled percentiles."""
    q = statistics.quantiles(pooled_z, n=100, method="inclusive")  # q[k-1] = k-th pct
    edges = [(q[p - 1], label) for p, label in PCT_EDGES]
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
    try:
        from database import Postgress
    except ImportError:
        sys.path.insert(
            0,
            os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "python_packages", "database")),
        )
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

    doi_to_authors = defaultdict(list)
    name_to_dois = defaultdict(set)
    for r in author_rows:
        doi_to_authors[r["doi"]].append(r["name"])
        name_to_dois[r["name"]].add(r["doi"])

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

    # --- Vooraf B: derive z-edges from pooled standardized z ----------------
    pooled_z = [p["z"] for p in papers.values() if p["z"] is not None]
    if not pooled_z:
        raise SystemExit("No usable z values; cannot derive bin edges.")
    z_edges = build_z_edges(pooled_z)
    bin_z = make_bin_z(z_edges)

    q = statistics.quantiles(pooled_z, n=100, method="inclusive")
    percentiles = {f"p{p}": round(q[p - 1], 4) for p in (1, 2, 5, 10, 15, 25, 50)}

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

    author_index = {}
    for name, dois in name_to_dois.items():
        kept = sorted(d for d in dois if d in papers_out)
        if kept:
            author_index[name] = kept

    suspects_out = {n: author_index.get(n, []) for n in SUSPECTS}

    # --- report ------------------------------------------------------------
    print(f"\nCorpus: {len(papers_out)} usable papers, {len(author_index)} authors with >=1 usable paper, "
          f"{len(journals_out)} journal baselines."
          + (f"  (restricted to journal {args.journal})" if args.journal else ""))
    print("Suspect anchor (usable papers each): "
          + ", ".join(f"{n!r}={len(suspects_out[n])}" for n in SUSPECTS))

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
        "suspects": suspects_out,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2, default=str)
    print(f"\nWrote {out_path}")


if __name__ == "__main__":
    main()
