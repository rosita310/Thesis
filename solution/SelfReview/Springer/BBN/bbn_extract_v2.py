"""
BBN V2 baseline extraction for the self-review case study (SQ1.1).

V2 model = per-gap (plate) network. For each gap an author has, the network
instantiates one gap node whose parents are (is_fraudster, article_type, pages).
Gaps are the conditionally-independent unit of evidence (fixes V1's
double-counting), and each gap is judged by its OWN journal's CPT.

This script produces the data both halves of V2 need:

  1. The JOURNAL-SPECIFIC honest baseline  P(gap-bin | type, pages)  for every
     journal, from that journal's own papers (suspects excluded). This is the
     `is_fraudster = false` row of the tied gap-CPT; the `= true` row is built
     at inference time as the alpha-mixture  alpha*manip + (1-alpha)*honest.

  2. Each investigated author's FULL list of gaps across all journals, with each
     gap's within-journal z, z-bin, article-type bin and pages bin -- the
     evidence fed to the per-gap inference.

z transform & per-journal reference are identical to V1:
  transform = ln(max(gap_days, GAP_FLOOR_DAYS)); per-journal mean/std (sample),
  high outliers (>HIGH_GAP_CUTOFF_DAYS) trimmed from the reference.

Run from this directory (BBN/) with the project venv:
    python bbn_extract_v2.py
"""

from __future__ import annotations

import configparser
import json
import math
import os
import statistics
import sys
from collections import defaultdict

try:
    from database import Postgress
except ImportError:
    sys.path.insert(
        0,
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "python_packages", "database")),
    )
    from database import Postgress


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SCHEMA = "springer"
JOURNAL_ID = "10623"            # the outlier's journal (only affects the printed summary)

SUSPECTS = {
    "R. Radheshwar",
    "Dibyendu Roy",
    "Pantelimon Stănică",
}

HIGH_GAP_CUTOFF_DAYS = 924
GAP_FLOOR_DAYS = 0.5
MIN_JOURNAL_REF = 2             # journal needs >=2 usable gaps (and std>0) to define z

# z-bins, refined on the fast (left) tail so the model has resolution where
# fraud lives. Edges are upper z-thresholds, fastest first; easy to retune.
# `mild_fast` lets a systematically (not just extremely) fast author accumulate
# evidence. The slow side stays coarse — fraud never makes a review slower.
Z_EDGES = [(-4.0, "very_extreme"), (-2.0, "extreme"), (-1.0, "mild_fast"), (float("inf"), "typical")]
Z_BINS = ["typical", "mild_fast", "extreme", "very_extreme"]
def bin_z(z):
    for edge, label in Z_EDGES:
        if z <= edge:
            return label
    return Z_BINS[0]

# per-gap context bins (parents of each gap node)
FAST_TYPE_KEYWORDS = (
    "editorial", "erratum", "correction", "corrigendum", "comment",
    "letter", "preface", "introduction", "book review", "obituary",
    "addendum", "retraction", "foreword", "in memoriam",
)
SHORT_PAGES_MAX = 4

ENV_PATH = "../../../.env"      # run from BBN/ (matches the V1 scripts)
OUT_DIR = os.path.join(os.path.dirname(__file__), "bbn_baselines")
OUT_PATH = os.path.join(OUT_DIR, f"bbn_v2_baseline_{JOURNAL_ID}.json")


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

    # --- per-paper transform + per-journal reference -----------------------
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
        p["z_bin"] = bin_z(p["z"]) if p["z"] is not None else None
        p["pages_bin"] = pages_bin(p["pages"])

    print(f"Usable gaps: {len(papers)} (0-day floored: {zero_gaps}; negative excluded: {neg_gaps}); "
          f"journals with z reference: {len(journal_ref)}")

    # --- pooled honest z distribution (to judge / retune the bin edges) ----
    honest_z = [p["z"] for p in papers.values()
                if p["z"] is not None
                and not any(n in SUSPECTS for n in doi_to_authors.get(p["doi"], ()))]
    if honest_z:
        q = statistics.quantiles(honest_z, n=100, method="inclusive")
        pct = {p: q[p - 1] for p in (1, 2, 5, 10, 15, 25, 50)}
        print("Pooled honest z percentiles: "
              + ", ".join(f"p{p}={v:+.2f}" for p, v in pct.items()))
        pooled_bins = defaultdict(int)
        for z in honest_z:
            pooled_bins[bin_z(z)] += 1
        n = len(honest_z)
        print("Pooled honest bin shares:   "
              + ", ".join(f"{b}={pooled_bins[b]/n:.4f}" for b in Z_BINS))

    # --- JOURNAL-SPECIFIC honest baseline  P(z-bin | type, pages) ----------
    # counts[journal_id][stratum_key][z_bin]; suspect-authored papers excluded.
    counts = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    for p in papers.values():
        if p["z_bin"] is None:
            continue
        if any(n in SUSPECTS for n in doi_to_authors.get(p["doi"], ())):
            continue
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

    # --- investigated authors: full gap list across all journals -----------
    suspects_out = {}
    for name in SUSPECTS:
        gaps = []
        for d in name_to_dois.get(name, ()):
            p = papers.get(d)
            if p and p["z"] is not None:
                gaps.append({
                    "doi": d, "journal_id": p["journal_id"], "gap_days": p["gap"],
                    "z": round(p["z"], 3), "z_bin": p["z_bin"],
                    "type_bin": p["type_bin"], "pages_bin": p["pages_bin"],
                    "article_type": p["article_type"],
                })
        gaps.sort(key=lambda g: g["z"])
        suspects_out[name] = {"n_gaps": len(gaps),
                              "journals": sorted({g["journal_id"] for g in gaps}),
                              "gaps": gaps}

    # --- report ------------------------------------------------------------
    print(f"\n=== Honest baseline for the outlier's journal {JOURNAL_ID} "
          f"(counts; suspects excluded) ===")
    jb = journals_out.get(JOURNAL_ID, {}).get("baseline_counts", {})
    for sk in sorted(jb):
        tot = sum(jb[sk].values())
        frac = {b: jb[sk][b] / tot for b in Z_BINS} if tot else {}
        print(f"  {sk:<22} n={tot:<5} " + ", ".join(f"{b}={frac.get(b,0):.4f}" for b in Z_BINS))

    print(f"\n=== Investigated authors: every gap (sorted most→least extreme) ===")
    for name, s in suspects_out.items():
        print(f"  {name!r}: {s['n_gaps']} gaps across {len(s['journals'])} journals")
        for g in s["gaps"][:8]:
            print(f"       z={g['z']:>7}  {g['z_bin']:<12} j={g['journal_id']:<7} "
                  f"type={g['type_bin']:<11} pages={g['pages_bin']:<7} {g['doi']}")
        if s["n_gaps"] > 8:
            print(f"       ... (+{s['n_gaps'] - 8} more)")

    os.makedirs(OUT_DIR, exist_ok=True)
    out = {
        "model": "v2_per_gap",
        "journal_id": JOURNAL_ID,
        "config": {
            "high_gap_cutoff_days": HIGH_GAP_CUTOFF_DAYS, "gap_floor_days": GAP_FLOOR_DAYS,
            "min_journal_ref": MIN_JOURNAL_REF,
            "z_edges": [[e if e != float("inf") else "inf", lbl] for e, lbl in Z_EDGES],
            "z_bins": Z_BINS,
            "short_pages_max": SHORT_PAGES_MAX,
        },
        "journals": journals_out,
        "suspects": suspects_out,
    }
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2, default=str)
    print(f"\nWrote {OUT_PATH}  ({len(journals_out)} journals' baselines + {len(suspects_out)} suspects)")


if __name__ == "__main__":
    main()
