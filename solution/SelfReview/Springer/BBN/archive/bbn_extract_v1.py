"""
BBN baseline extraction for the self-review case study (SQ1.1).

Computes the author-level gap statistics that feed the v1 Bayesian Belief
Network for one journal's outlier, plus the peer-group distributions needed to
(a) choose the discretization cut-points and (b) fill the `Is fraudster = false`
rows of every CPT.

Network being served (v1):

        [article_type] [pages]        [Is fraudster]
                \        |         /         |       \
                 v       v        v          v        v
                   [Min log-z]        [Median log-z]   [Consistency]
                                                   ( N gaps -> confidence flag )

Scope decisions:
  * Context = ALL 143 Springer journals. Each author is characterised by their
    full cross-journal record, so prolific authors get real Median/Consistency
    signal instead of collapsing to a single paper.
  * z-score is computed WITHIN each paper's own journal (per-journal mean/std of
    ln(gap)), so a fast paper is judged against that journal's norm.
  * Peer group = all authors who published in the target journal (JOURNAL_ID),
    suspects excluded. Each peer's statistics still come from their full record.

Gap -> log scale:
  * gap = accepted - received (days), stored as articles.review_days
  * transform = ln( max(gap, GAP_FLOOR_DAYS) ); the floor handles 0-day gaps
    ("same calendar day" = under a day -> 0.5 day) without the +1 offset that
    would distort every value. Negative gaps are data errors -> excluded.
  * per journal: mean/std over gaps in [0, HIGH_GAP_CUTOFF_DAYS] (high outliers
    trimmed so a few multi-year gaps don't inflate std). Sample std (ddof=1).
  * z(paper) = (transform - mean_j) / std_j

Outputs a readable report to stdout and `bbn_baseline_<journal>.json`.

Run from this directory with the project venv:
    python bbn_extract_baseline.py
"""

from __future__ import annotations

import json
import math
import os
import statistics
import sys
import configparser
from collections import defaultdict

from database import Postgress


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SCHEMA = "springer"
JOURNAL_ID = "10623"            # Designs, Codes and Cryptography (the outlier's journal)

# Authors under investigation. Excluded from the peer-group distributions;
# reported separately as the evidence to feed the BBN.
SUSPECTS = {
    "R. Radheshwar",
    "Dibyendu Roy",
    "Pantelimon Stănică",
}

VERIFY_DOI = "10.1007/s10623-026-01861-7"   # the flagged paper, for a sanity print

HIGH_GAP_CUTOFF_DAYS = 924      # gaps > this are excluded from the per-journal z reference
GAP_FLOOR_DAYS = 0.5            # a 0-day gap = "under a day" -> ln(0.5); only touches zeros
MIN_JOURNAL_REF = 2             # a journal needs >=2 usable gaps (and std>0) to define z

# A year counts toward Consistency if it contains a gap at or below this z.
CONSISTENCY_Z = -2.0

# Coarse context discretizations for the Min log-z parents.
FAST_TYPE_KEYWORDS = (
    "editorial", "erratum", "correction", "corrigendum", "comment",
    "letter", "preface", "introduction", "book review", "obituary",
    "addendum", "retraction", "foreword", "in memoriam",
)
SHORT_PAGES_MAX = 4             # pages <= this -> "short" (a note/comment)

OUT_PATH = os.path.join(os.path.dirname(__file__), "bbn_baselines", f"bbn_baseline_{JOURNAL_ID}.json")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def read_config(path) -> configparser.SectionProxy:
    if not os.path.exists(path):
        sys.exit(
            f"\nERROR: no .env found at {path}\n"
            f"Copy code/env-example to code/.env and fill in the POSTGRES_* values.\n"
        )
    with open(path, "r") as f:
        config_string = "[SECTION]\n" + f.read()
    config = configparser.ConfigParser()
    config.read_string(config_string)
    return config["SECTION"]


def pages_of(first, last):
    if first is None or last is None or last < first:
        return None
    return last - first + 1


def type_bin(article_type: str | None) -> str:
    t = (article_type or "").lower()
    return "fast_type" if any(k in t for k in FAST_TYPE_KEYWORDS) else "normal_type"


def pages_bin(pages) -> str:
    if pages is None:
        return "unknown"
    return "short" if pages <= SHORT_PAGES_MAX else "normal"


def quantile_cuts(values, n=3):
    vals = [v for v in values if v is not None]
    if len(vals) < n:
        return []
    return statistics.quantiles(vals, n=n, method="inclusive")


def summarize(values):
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    out = {"n": len(vals), "min": min(vals), "median": statistics.median(vals),
           "max": max(vals), "mean": statistics.fmean(vals)}
    if len(vals) >= 10:
        deciles = statistics.quantiles(vals, n=10, method="inclusive")
        out["p10"], out["p90"] = deciles[0], deciles[8]
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    config = read_config('../../../.env')
    db = Postgress(
        server=config["POSTGRES_SERVER"],
        database=config["POSTGRES_DB"],
        user=config["POSTGRES_USER"],
        password=config["POSTGRES_PASSWORD"],
    )

    # --- load ALL articles + authors across all journals -------------------
    print("Loading all journals ...")
    articles = db.execute_query_result(f"""
        SELECT doi, journal_id, received, review_days, article_type, first_page, last_page
        FROM "{SCHEMA}"."articles"
    """)
    author_rows = db.execute_query_result(f"""
        SELECT doi, name, journal_id
        FROM "{SCHEMA}"."authors"
    """)

    # name -> set of DOIs (across all journals); names present in target journal
    name_to_dois = defaultdict(set)
    names_in_journal = set()
    for r in author_rows:
        name_to_dois[r["name"]].add(r["doi"])
        if r["journal_id"] == JOURNAL_ID:
            names_in_journal.add(r["name"])

    print(f"Loaded {len(articles)} articles across "
          f"{len({a['journal_id'] for a in articles})} journals; "
          f"{len(name_to_dois)} distinct author names. "
          f"Target journal {JOURNAL_ID}: {len(names_in_journal)} distinct authors.")

    # --- per-paper transform; count the edge cases -------------------------
    papers = {}
    neg_gaps = zero_gaps = 0
    journal_vals = defaultdict(list)     # journal_id -> [transform]
    for a in articles:
        gap = a["review_days"]
        if gap is None:
            continue
        if gap < 0:
            neg_gaps += 1
            continue
        if gap == 0:
            zero_gaps += 1
        if gap > HIGH_GAP_CUTOFF_DAYS:
            continue
        transform = math.log(max(gap, GAP_FLOOR_DAYS))
        papers[a["doi"]] = {
            "doi": a["doi"],
            "journal_id": a["journal_id"],
            "gap": gap,
            "t": transform,
            "year": a["received"].year if a["received"] else None,
            "article_type": a["article_type"],
            "type_bin": type_bin(a["article_type"]),
            "pages": pages_of(a["first_page"], a["last_page"]),
        }
        journal_vals[a["journal_id"]].append(transform)

    print(f"Usable gaps: {len(papers)} (0-day: {zero_gaps} floored to {GAP_FLOOR_DAYS}d; "
          f"negative excluded: {neg_gaps})")

    # --- per-journal z reference ------------------------------------------
    journal_ref = {}
    for jid, vals in journal_vals.items():
        if len(vals) < MIN_JOURNAL_REF:
            continue
        sd = statistics.stdev(vals)
        if sd == 0:
            continue
        journal_ref[jid] = (statistics.fmean(vals), sd)
    print(f"Journals with a usable z reference (>= {MIN_JOURNAL_REF} gaps, std>0): {len(journal_ref)}")

    for p in papers.values():
        ref = journal_ref.get(p["journal_id"])
        p["z"] = (p["t"] - ref[0]) / ref[1] if ref else None

    # --- sanity print on the flagged paper --------------------------------
    vp = papers.get(VERIFY_DOI)
    if vp and vp["z"] is not None:
        m, s = journal_ref[vp["journal_id"]]
        print(f"\n{VERIFY_DOI}: gap={vp['gap']}d -> z={vp['z']:.2f} "
              f"(journal {vp['journal_id']} mean ln-gap={m:.3f}, std={s:.3f}). "
              f"NB: differs from the manual -8.56, which used ln(gap+1).")
    else:
        print(f"\n{VERIFY_DOI}: no usable z.")

    # --- per-author aggregation over the FULL cross-journal record --------
    def author_stats(name):
        dois = [d for d in name_to_dois.get(name, ())
                if d in papers and papers[d]["z"] is not None]
        zs = [papers[d]["z"] for d in dois]
        if not zs:
            return None
        min_doi = min(dois, key=lambda d: papers[d]["z"])
        mp = papers[min_doi]
        years_out = {papers[d]["year"] for d in dois
                     if papers[d]["z"] <= CONSISTENCY_Z and papers[d]["year"]}
        years_active = {papers[d]["year"] for d in dois if papers[d]["year"]}
        journals = {papers[d]["journal_id"] for d in dois}
        return {
            "name": name,
            "n_gaps": len(zs),
            "n_journals": len(journals),
            "min_log_z": min(zs),
            "median_log_z": statistics.median(zs),
            "min_paper_doi": min_doi,
            "min_paper_journal": mp["journal_id"],
            "min_paper_type_bin": mp["type_bin"],
            "min_paper_pages_bin": pages_bin(mp["pages"]),
            "min_paper_article_type": mp["article_type"],
            "min_paper_pages": mp["pages"],
            "years_active": len(years_active),
            "n_outlying_periods": len(years_out),
        }

    peer_stats = [s for s in (author_stats(n) for n in names_in_journal
                              if n not in SUSPECTS) if s]

    # --- peer-group distributions (the CPT baseline) ----------------------
    print(f"\nPeer authors (published in {JOURNAL_ID}, suspects excluded, "
          f">=1 usable gap): {len(peer_stats)}")

    print("\n--- Peer Min log-z ---")
    print("  ", summarize([s["min_log_z"] for s in peer_stats]))
    print("   tertile cuts:", [round(c, 3) for c in quantile_cuts([s["min_log_z"] for s in peer_stats])])

    print("\n--- Peer Median log-z ---")
    print("  ", summarize([s["median_log_z"] for s in peer_stats]))
    print("   tertile cuts:", [round(c, 3) for c in quantile_cuts([s["median_log_z"] for s in peer_stats])])

    print("\n--- Peer n_gaps (now cross-journal) ---")
    print("  ", summarize([s["n_gaps"] for s in peer_stats]))
    multi = sum(1 for s in peer_stats if s["n_gaps"] >= 3)
    print(f"   peers with >=3 gaps: {multi} ({100*multi/len(peer_stats):.0f}%) "
          f"-- these are where Median/Consistency carry signal")

    print("\n--- Min-paper context mix among peers (Min log-z CPT sparsity) ---")
    ctab = defaultdict(int)
    for s in peer_stats:
        ctab[(s["min_paper_type_bin"], s["min_paper_pages_bin"])] += 1
    for k in sorted(ctab):
        print(f"   type={k[0]:<12} pages={k[1]:<8} -> {ctab[k]}")

    print("\n--- Distinct article_type values (all journals; for FAST_TYPE tuning) ---")
    tc = defaultdict(int)
    for p in papers.values():
        tc[p["article_type"]] += 1
    for t, c in sorted(tc.items(), key=lambda x: -x[1])[:25]:
        print(f"   {c:>6}  {t!r} -> {type_bin(t)}")

    # --- the suspects ------------------------------------------------------
    print("\n=== INVESTIGATED AUTHORS (evidence to feed the BBN) ===")
    suspect_report = {}
    for name in SUSPECTS:
        s = author_stats(name)
        if s is None:
            cands = [n for n in name_to_dois if name.split()[-1].lower() in n.lower()]
            print(f"   {name!r}: NOT FOUND. candidates: {cands[:10]}")
            suspect_report[name] = None
            continue
        print(f"   {name!r}: n_gaps={s['n_gaps']} across {s['n_journals']} journals, "
              f"min_log_z={s['min_log_z']:.2f}, median_log_z={s['median_log_z']:.2f}, "
              f"outlying_periods={s['n_outlying_periods']}/{s['years_active']}")
        print(f"       worst: {s['min_paper_doi']} (journal {s['min_paper_journal']}) "
              f"[type={s['min_paper_article_type']!r}->{s['min_paper_type_bin']}, "
              f"pages={s['min_paper_pages']}->{s['min_paper_pages_bin']}]")
        suspect_report[name] = s

    # --- machine-readable dump --------------------------------------------
    out = {
        "journal_id": JOURNAL_ID,
        "config": {
            "high_gap_cutoff_days": HIGH_GAP_CUTOFF_DAYS,
            "gap_floor_days": GAP_FLOOR_DAYS,
            "consistency_z": CONSISTENCY_Z,
            "short_pages_max": SHORT_PAGES_MAX,
            "min_journal_ref": MIN_JOURNAL_REF,
        },
        "counts": {"usable_gaps": len(papers), "zero_gaps": zero_gaps,
                   "negative_gaps": neg_gaps, "journals_with_ref": len(journal_ref)},
        "verify": {"doi": VERIFY_DOI, "z": vp["z"] if vp else None},
        "peer_stats": peer_stats,
        "suspects": suspect_report,
    }
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2, default=str)
    print(f"\nWrote {OUT_PATH}")


if __name__ == "__main__":
    main()
