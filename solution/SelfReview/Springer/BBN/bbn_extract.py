"""
BBN corpus extraction for the self-review case study (SQ1.1).

Builds the data that bbn_infer.py needs, from the PostgreSQL `springer` schema.
The model is a per-gap Bayesian network: one latent node G = is_genuine per
author, and for each review-time "gap" (days from submission to acceptance) one
observed node hanging off G with covariate parents (article_type, page_class).
Gaps are the conditionally-independent unit of evidence.

Per paper: gap_days (d_i) -> log_gap (x_i) =
ln(max(gap_days, GAP_FLOOR_DAYS)), standardized within its own journal to
z = (log_gap - ref_mean_j) / ref_std_j (mu_j, sigma_j; sample stats). A journal
with fewer than Z_REFERENCE_MIN_N gaps (or zero spread) falls back to the corpus
reference: mu and sigma of the log gap pooled over all journals. Every journal
records which one it used (`z_reference`: "journal" | "corpus"). z is
discretized with edges set to the pooled percentiles of z over all journals --
fast (left) tail refined, slow side coarse:
    ultra_extreme <= p0.1 < very_extreme <= p1 < extreme <= p5
    < mild_fast <= p15 < typical
The edges used are written to config.z_edges, so inference reads the precomputed
z_bin and never re-bins.

Papers above the ceiling (gap_days > GAP_CEILING_DAYS, tau) are kept in the
corpus with `above_ceiling: true`. They get a z from their journal's reference
and a bin from the unchanged edges (always TYPICAL), count in their authors'
records (n_gaps, n_min, LR) and count in the genuine baseline (the per-bin peer
counts). They are left out of what shapes z and the bins: mu_j and sigma_j, the
corpus reference, the gap count that decides between them, and the pooled
percentiles behind the bin edges, so the slow tail cannot bend them.

Output (one corpus JSON in bbn_baselines/):
    journals      -- per-journal genuine baseline counts P(bin | article_type, page_class),
                     over every usable paper, above-ceiling ones included (no author
                     exclusion here; leave-one-author exclusion is applied at
                     inference time)
    papers        -- one entry per usable DOI (journal, gap_days, z, z_bin,
                     article_type, page_class, above_ceiling)
    author_index  -- author identity -> [doi, ...] (cross-journal)
    author_labels -- identity -> representative display name
    suspects      -- the 3 case-study names, as a validation anchor

Author identity is a hybrid ORCID-or-name key: a row's ORCID (from
springer.author_orcid) if present; propagated to that author's ORCID-less rows
when the name maps to exactly one ORCID corpus-wide; else the name string. Falls
back to name-only identities if springer.author_orcid is absent.

Run from this directory (BBN/) with the scraper venv (DB via pyodbc):
    python bbn_extract.py                 # whole corpus
    python bbn_extract.py --journal 10623 # restrict to one journal's authors
"""

from __future__ import annotations

import argparse
import configparser
import json
import math
import os
import re
import statistics
import sys
from collections import Counter, defaultdict


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SCHEMA = "springer"

GAP_CEILING_DAYS = 924          # tau: longer gaps stay out of mu_j/sigma_j and the edges
GAP_FLOOR_DAYS = 0.5            # delta: floor on the gap, keeps zero-day gaps finite
Z_REFERENCE_MIN_N = 30          # gaps <= tau a journal needs (and std > 0) for its own
                                # mu_j/sigma_j; below it, z falls back to the corpus
                                # reference. Mirrors FALLBACK_MIN_N in bbn_infer.py.

# z-bin edges are the pooled percentiles of the standardized log-gap z.
# Edges are upper z-thresholds, fastest first. Refined on the fast (left) tail so
# the model has resolution where manipulation lives; the slow side stays coarse
# (manipulation never makes a review slower). The deep tail is split with an extra
# p0.1 cut (`ultra_extreme`) so a corpus-level outlier (e.g. z=-9) is no longer
# lumped with a merely-p1 gap (z~-2.9).
Z_BINS = ["typical", "mild_fast", "extreme", "very_extreme", "ultra_extreme"]
PCT_EDGES = [(0.1, "ultra_extreme"), (1, "very_extreme"), (5, "extreme"), (15, "mild_fast")]


# per-gap covariates (parents of each gap node): article type t_i and page class p_i
FAST_TYPE_KEYWORDS = (
    "editorial", "erratum", "correction", "corrigendum", "comment",
    "letter", "preface", "introduction", "book review", "obituary",
    "addendum", "retraction", "foreword", "in memoriam",
)


# Editorial-signalling TITLE patterns, used to relabel papers whose type metadata
# (the DB's `article_type` field) is generic (e.g. "Article") but whose title marks
# them as editorial / non-peer-reviewed content -- the leak the field-only classifier
# misses (a same-day "Editorial: ..." filed as an Article looks incriminating).
# Matched as a PREFIX (case-insensitive, leading quote/paren/space tolerated) so
# genuine research titles ("An introduction to ...") are never caught: precision
# matters because a false relabelling would HIDE a real fast gap.
FAST_TITLE_PATTERNS = (
    r"(guest\s+)?editorial\b",
    r"editor['’`s]*\s+(note|introduction|message|perspective|comment)\b",
    r"from\s+the\s+editors?\b",
    r"preface\b",
    r"foreword\b",
    r"in\s+memoriam\b",
    r"obituar(y|ies)\b",
    r"erratum\b",
    r"corrigendum\b",
    r"correction\s+to\b",
    r"retraction\s+note\b",
    r"retraction:",
    r"addendum\b",
    r"comments?\s+on\b",
    r"reply\s+to\b",
    r"response\s+to\s+(the\s+)?comment",
    r"introduction\s+to\s+the\s+special\s+(issue|section)\b",
    r"special\s+(issue|section)\s*(?::|on\b)",
)
_FAST_TITLE_RE = re.compile(
    r"^\s*[\"'`(\[]?\s*(?:" + "|".join(FAST_TITLE_PATTERNS) + r")", re.IGNORECASE)

SHORT_PAGES_MAX = 4

ENV_PATH = "../../../.env"      # run from BBN/
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


def page_count_of(first, last):
    if first is None or last is None or last < first:
        return None
    return last - first + 1


def title_is_fast(title):
    """True if the *title* signals editorial / non-peer-reviewed content.

    Anchored at the start of the title (leading quote/paren/space tolerated), so
    "Editorial: ..." / "Correction to: ..." match while a research paper titled
    "An introduction to ..." does not. Catches items whose type metadata is
    generic but that are editorial in nature."""
    return bool(title) and _FAST_TITLE_RE.match(title) is not None


def article_type_of(type_metadata, title=None):
    """Article type t_i (fast_type / normal_type) from the raw type metadata."""
    t = (type_metadata or "").lower()
    if any(k in t for k in FAST_TYPE_KEYWORDS):
        return "fast_type"
    if title_is_fast(title):
        return "fast_type"
    return "normal_type"


def page_class_of(page_count):
    """Page class p_i (short / normal / unknown) from the page count."""
    if page_count is None:
        return "unknown"
    return "short" if page_count <= SHORT_PAGES_MAX else "normal"


def context_key(article_type, page_class):
    """Key of one conditioning context (article type, page class) in baseline_counts."""
    return f"{article_type}|{page_class}"


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


def corpus_reference(papers):
    """(mu, sigma) of the log gap pooled over every journal's gaps <= tau, or None
    if the corpus has fewer than two such gaps or no spread."""
    vals = [p["log_gap"] for p in papers.values() if not p["above_ceiling"]]
    if len(vals) < 2:
        return None
    sd = statistics.stdev(vals)
    return (statistics.fmean(vals), sd) if sd > 0 else None


def journal_reference(papers):
    """{journal_id: (mu, sigma, source)}: the z reference of every journal in `papers`.

    source "journal": the journal's own mu_j/sigma_j, used when it has at least
    Z_REFERENCE_MIN_N gaps <= tau and a positive std. Otherwise source "corpus": the
    fallback to corpus_reference(). Above-ceiling papers never enter either
    reference (nor the gap count that chooses between them), so they cannot bend z.
    A journal is left without a reference only if the corpus reference is None."""
    log_gaps = defaultdict(list)
    for p in papers.values():
        log_gaps[p["journal_id"]]                       # every journal gets an entry
        if not p["above_ceiling"]:
            log_gaps[p["journal_id"]].append(p["log_gap"])
    corpus = corpus_reference(papers)
    ref = {}
    for jid, vals in log_gaps.items():
        sd = statistics.stdev(vals) if len(vals) >= Z_REFERENCE_MIN_N else 0.0
        if sd > 0:
            ref[jid] = (statistics.fmean(vals), sd, "journal")
        elif corpus is not None:
            ref[jid] = (corpus[0], corpus[1], "corpus")
    return ref


def edge_sample(papers):
    """Ascending z of the gaps <= tau: the pooled sample behind the bin edges."""
    return sorted(p["z"] for p in papers.values()
                  if p["z"] is not None and not p["above_ceiling"])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="BBN corpus extraction.")
    parser.add_argument("--journal", default=None,
                        help="restrict the emitted papers/author_index to one journal_id "
                             "(the genuine baselines are always corpus-wide).")
    parser.add_argument("--selftest", action="store_true",
                        help="check the ceiling rule on synthetic papers (no DB) and exit")
    args = parser.parse_args()
    if args.selftest:
        return selftest()

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
        SELECT doi, journal_id, received, review_days, article_type, title, first_page, last_page
        FROM "{SCHEMA}"."articles"
    """)
    author_rows = db.execute_query_result(f"""
        SELECT doi, name, journal_id
        FROM "{SCHEMA}"."authors"
    """)

    name_to_dois = defaultdict(set)
    for r in author_rows:
        name_to_dois[r["name"]].add(r["doi"])

    # ----- ORCID identity: hybrid key = ORCID where known, else name -----
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
    # The DB column `review_days` is the gap d_i; `article_type` is the raw type metadata.
    papers = {}
    missing_gaps = neg_gaps = zero_gaps = 0
    n_title_relabelled = 0
    title_relabel_sample = []
    for a in articles:
        gap_days = a["review_days"]
        if gap_days is None:       # no received/accepted date -> no computable gap
            missing_gaps += 1
            continue
        if gap_days < 0:           # acceptance before submission (verified: ~none in corpus)
            neg_gaps += 1
            continue
        if gap_days == 0:
            zero_gaps += 1
        # above the ceiling: kept out of mu_j/sigma_j and the edges below, but counted
        # in the authors' records and in the baseline counts
        above_ceiling = gap_days > GAP_CEILING_DAYS
        log_gap = math.log(max(gap_days, GAP_FLOOR_DAYS))
        title = a["title"]
        type_from_field = article_type_of(a["article_type"])  # type metadata only
        article_type = ("fast_type" if (type_from_field == "fast_type" or title_is_fast(title))
                        else "normal_type")
        via_title = type_from_field == "normal_type" and article_type == "fast_type"
        if via_title:
            n_title_relabelled += 1
            if len(title_relabel_sample) < 20:
                title_relabel_sample.append((a["doi"], a["article_type"], title))
        papers[a["doi"]] = {
            "doi": a["doi"], "journal_id": a["journal_id"], "gap_days": gap_days,
            "log_gap": log_gap,
            "type_metadata": a["article_type"], "title": title,
            "article_type": article_type, "relabelled_by_title": via_title,
            "page_count": page_count_of(a["first_page"], a["last_page"]),
            "above_ceiling": above_ceiling,
        }

    # z reference per journal: its own mu_j/sigma_j, or the corpus fallback when it has
    # fewer than Z_REFERENCE_MIN_N gaps <= tau
    journal_ref = journal_reference(papers)
    corpus_ref = corpus_reference(papers)

    for p in papers.values():
        ref = journal_ref.get(p["journal_id"])
        p["z"] = (p["log_gap"] - ref[0]) / ref[1] if ref else None
        p["page_class"] = page_class_of(p["page_count"])

    # --- derive z-edges from pooled standardized z (gaps <= tau only) ----------
    sorted_z = edge_sample(papers)
    if not sorted_z:
        raise SystemExit("No usable z values; cannot derive bin edges.")
    z_edges = build_z_edges(sorted_z)
    bin_z = make_bin_z(z_edges)

    percentiles = {f"p{p}": round(percentile(sorted_z, p), 4)
                   for p in (0.1, 0.5, 1, 2, 5, 10, 15, 25, 50)}

    for p in papers.values():
        p["z_bin"] = bin_z(p["z"]) if p["z"] is not None else None

    usable = [p for p in papers.values() if p["z_bin"] is not None]
    below = [p for p in usable if not p["above_ceiling"]]          # shape mu_j/sigma_j and the edges
    above = [p for p in usable if p["above_ceiling"]]
    not_typical_above = [p for p in above if p["z_bin"] != "typical"]
    assert not not_typical_above, (
        f"{len(not_typical_above)} above-ceiling papers fall outside TYPICAL, e.g. "
        f"{not_typical_above[0]['doi']} z={not_typical_above[0]['z']:.3f}")
    print(f"Usable gaps: {len(usable)} = {len(below)} at or below the ceiling + {len(above)} "
          f"above it (0-day floored: {zero_gaps}; missing date excluded: "
          f"{missing_gaps}; negative excluded: {neg_gaps}); journals with z reference: "
          f"{len(journal_ref)}")
    for source in ("journal", "corpus"):
        jids = {j for j, r in journal_ref.items() if r[2] == source}
        n_gaps = sum(1 for p in usable if p["journal_id"] in jids)
        n_below = sum(1 for p in below if p["journal_id"] in jids)
        print(f"z reference '{source}': {len(jids)} journals, {n_gaps} usable gaps "
              f"({n_below} at or below the ceiling)")
    if corpus_ref:
        print(f"Corpus reference (fallback below {Z_REFERENCE_MIN_N} gaps): "
              f"mu={corpus_ref[0]:.4f} sigma={corpus_ref[1]:.4f}")
    if above:
        print(f"Above-ceiling papers: all {len(above)} in typical; lowest z = "
              f"{min(p['z'] for p in above):+.3f}")
    print("Pooled z percentiles: " + ", ".join(f"{k}={v:+.2f}" for k, v in percentiles.items()))
    print("z-edges in use:       "
          + ", ".join(f"{lbl}<=({e:+.3f})" if e != float('inf') else f"{lbl}=rest"
                      for e, lbl in z_edges))
    for label, group in (("gaps <= ceiling, define the edges", below),
                         ("all usable gaps, the genuine baseline", usable)):
        pooled_bins = defaultdict(int)
        for p in group:
            pooled_bins[p["z_bin"]] += 1
        print("Pooled bin shares:    "
              + ", ".join(f"{b}={pooled_bins[b]/len(group):.4f}" for b in Z_BINS)
              + f"  ({label})")

    # Editorial-title leak: papers rescued from a wrong "normal_type" baseline.
    # rescued_fast are the actual false positives this fix removes -- editorial
    # content that would otherwise have contributed an incriminating fast-bin LR.
    rescued = [p for p in usable if p["relabelled_by_title"]]
    fast_bins = ("extreme", "very_extreme", "ultra_extreme")
    rescued_fast = [p for p in rescued if p["z_bin"] in fast_bins]
    print(f"Editorial-title relabelling: {n_title_relabelled} papers filed under a "
          f"non-fast type metadata but flagged editorial by title;")
    n_rescued_above = sum(1 for p in rescued if p["above_ceiling"])
    print(f"   {len(rescued)} are usable gaps ({n_rescued_above} above the ceiling), of which "
          f"{len(rescued_fast)} fall in a fast bin (the false positives this fix removes).")
    for doi, at, tt in title_relabel_sample[:15]:
        shown = (tt[:70] + "...") if tt and len(tt) > 70 else (tt or "")
        print(f"     [{at or '?'}] {shown}  ({doi})")

    # --- JOURNAL-SPECIFIC genuine baseline counts P(bin | article_type, page_class) --
    # Every usable paper counts, above-ceiling ones included (they only stay out of
    # mu_j/sigma_j and the edges). Leave-one-author exclusion is applied at inference
    # time. counts[journal_id][context_key][z_bin].
    counts = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    for p in usable:
        counts[p["journal_id"]][context_key(p["article_type"], p["page_class"])][p["z_bin"]] += 1

    journals_out = {}
    for jid, ref in journal_ref.items():
        baseline = {context: {b: bins.get(b, 0) for b in Z_BINS}
                    for context, bins in counts.get(jid, {}).items()}
        journals_out[jid] = {
            "ref_mean": ref[0], "ref_std": ref[1],            # mu_j, sigma_j of the log gap
            "z_reference": ref[2],                            # "journal" | "corpus" (fallback)
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
            "journal_id": p["journal_id"], "gap_days": p["gap_days"],
            "z": round(p["z"], 3), "z_bin": p["z_bin"],
            "article_type": p["article_type"], "page_class": p["page_class"],
            "type_metadata": p["type_metadata"], "title": p["title"],
            "relabelled_by_title": p["relabelled_by_title"],
            "above_ceiling": p["above_ceiling"],
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
        OUT_DIR, f"bbn_corpus{('_' + args.journal) if args.journal else ''}.json")
    out = {
        "model": "per_gap_corpus",
        "journal_filter": args.journal,
        "config": {
            "gap_ceiling_days": GAP_CEILING_DAYS, "gap_floor_days": GAP_FLOOR_DAYS,
            "z_reference_min_n": Z_REFERENCE_MIN_N,
            "corpus_ref_mean": corpus_ref[0] if corpus_ref else None,
            "corpus_ref_std": corpus_ref[1] if corpus_ref else None,
            "z_edges": [[e if e != float("inf") else "inf", lbl] for e, lbl in z_edges],
            "z_bins": Z_BINS,
            "z_percentiles": percentiles,
            "short_pages_max": SHORT_PAGES_MAX,
            "fast_title_patterns": list(FAST_TITLE_PATTERNS),
            "n_title_relabelled": n_title_relabelled,
        },
        "journals": journals_out,
        "papers": papers_out,
        "author_index": author_index,
        "author_labels": author_labels,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2, default=str)
    print(f"\nWrote {out_path}")


# ---------------------------------------------------------------------------
# Self-test (synthetic papers; no DB needed)
# ---------------------------------------------------------------------------

def selftest():
    """The z reference: own mu_j/sigma_j from Z_REFERENCE_MIN_N gaps <= tau, else the
    corpus fallback; an above-tau paper never moves either reference or the edges."""
    ok = True

    def check(name, cond):
        nonlocal ok
        print(f"  [{'PASS' if cond else 'FAIL'}] {name}")
        ok = ok and cond

    def paper(doi, jid, gap_days):
        return {"doi": doi, "journal_id": jid, "gap_days": gap_days,
                "log_gap": math.log(max(gap_days, GAP_FLOOR_DAYS)),
                "above_ceiling": gap_days > GAP_CEILING_DAYS}

    n = Z_REFERENCE_MIN_N
    below = {f"P{i}": paper(f"P{i}", "J1", 10 + 15 * i) for i in range(n)}     # exactly n
    below.update({f"Q{i}": paper(f"Q{i}", "J2", d) for i, d in enumerate([10, 40, 80, 900])})
    below.update({f"R{i}": paper(f"R{i}", "J4", 20 + 7 * i) for i in range(n - 1)})  # n - 1
    below.update({f"F{i}": paper(f"F{i}", "J5", 60) for i in range(n)})       # no spread
    slow = {"S1": paper("S1", "J1", 2000), "S2": paper("S2", "J2", 5000),
            "S3": paper("S3", "J3", 3000)}       # J3 holds only an above-ceiling paper
    slow.update({f"T{i}": paper(f"T{i}", "J4", 1500 + i) for i in range(3)})   # J4 reaches n
    both = {**below, **slow}

    allv = [p["log_gap"] for p in below.values()]
    corpus = (statistics.fmean(allv), statistics.stdev(allv))
    ref_b, ref_a = journal_reference(below), journal_reference(both)
    j1 = [p["log_gap"] for p in below.values() if p["journal_id"] == "J1"]
    check(f"a journal with exactly {n} gaps gets its own mu_j/sigma_j",
          ref_a["J1"] == (statistics.fmean(j1), statistics.stdev(j1), "journal"))
    check(f"a journal with fewer than {n} gaps gets the corpus mu/sigma",
          ref_a["J2"] == (*corpus, "corpus") and corpus_reference(both) == corpus)
    check(f"above-ceiling papers do not count toward the {n} (J4: {n - 1} + 3 -> corpus)",
          ref_a["J4"][2] == "corpus")
    check("a journal with zero spread falls back to the corpus", ref_a["J5"][2] == "corpus")
    check("a journal with only above-ceiling papers gets the corpus reference",
          ref_a["J3"] == (*corpus, "corpus"))
    check("above-ceiling papers never move mu_j/sigma_j or the corpus reference",
          all(ref_a[j] == ref_b[j] for j in ref_b) and corpus_reference(both) == corpus_reference(below))

    for ps in (below, both):
        for p in ps.values():
            r = ref_a[p["journal_id"]]
            p["z"] = (p["log_gap"] - r[0]) / r[1]
    check("above-ceiling papers never move the edge sample",
          edge_sample(below) == edge_sample(both))
    edges = build_z_edges(edge_sample(both))
    check("... so the bin edges are identical", edges == build_z_edges(edge_sample(below)))
    bin_z = make_bin_z(edges)
    check("above-ceiling papers fall in TYPICAL", all(bin_z(p["z"]) == "typical" for p in slow.values()))

    print("\nSELFTEST:", "ALL PASS" if ok else "FAILURES PRESENT")
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
