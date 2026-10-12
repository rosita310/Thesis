"""
Matching authors based on ORCIDs instead of names -- step 2 of 2: match DBLP ORCIDs to springer authors.

Reads:
  * springer.authors  (doi, name)               -- the scraped author rows
  * dblp_orcid_raw.tsv (doi, dblp_name, ordinal, orcid)  -- produced by
    dblp_orcid_extract.py from the local DBLP RDF store

For each DOI present in both, it aligns the two author lists on a normalized
name key (diacritics folded, DBLP homonym suffix stripped, tokens sorted so
"Zeyd Boukhers" == "Boukhers Zeyd") -- low risk because the match is confined to
a single paper -- and assigns the DBLP ORCID to the matching springer author.

Writes a NON-DESTRUCTIVE mapping table (rebuilt each run):
  springer.author_orcid(doi, springer_name, dblp_name, orcid, ordinal,
                         match_method, retrieved_at)
match_method in {matched_orcid, matched_no_orcid, matched_orcid_loose,
                 matched_no_orcid_loose, ambiguous_match, no_dblp_match}.
The `_loose` methods come from position + (surname & first-initial) matching and
are flagged so they can be trusted/audited separately from exact matches.

The BBN grouping key then becomes hybrid: ORCID where present, else the name
string (handled in bbn_extract -- a later step). This script only builds the map
and reports coverage.

Run from BBN/ with the scraper venv (DB via pyodbc):
    python dblp_orcid_match.py --tsv dblp_orcid_raw.tsv
    python dblp_orcid_match.py --selftest        # name-matching validation, no DB
"""

from __future__ import annotations

import argparse
import configparser
import csv
import os
import re
import sys
import unicodedata
from collections import defaultdict

SCHEMA = "springer"
ENV_PATH = "../../../.env"
OUT_TABLE = "author_orcid"

_HOMONYM_SUFFIX = re.compile(r"\s+\d{4}$")     # DBLP disambiguation suffix, e.g. "Wei Wang 0001"
_NON_ALNUM = re.compile(r"[^a-z0-9]+")


# ---------------------------------------------------------------------------
# Name normalization & matching  (pure -- importable & unit-tested)
# ---------------------------------------------------------------------------

def _norm_ordered(name):
    """Normalized, ORDER-PRESERVING token list (diacritics folded, homonym suffix
    stripped). Order is kept so the surname (last token) is identifiable."""
    if not name:
        return []
    s = _HOMONYM_SUFFIX.sub("", name.strip())
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))   # drop diacritics
    s = _NON_ALNUM.sub(" ", s.lower())
    return [t for t in s.split() if t]


def norm_tokens(name):
    """Order-independent normalized key ('Zeyd Boukhers' == 'Boukhers, Zeyd')."""
    return tuple(sorted(_norm_ordered(name)))


def _to_int(v):
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        return None


def loose_compatible(a, b):
    """Same surname (last token) + same first given-name initial.

    Springer and DBLP both print names First..Last, so the last token is the
    surname. This is used ONLY in combination with an equal author position --
    on one paper that makes a surname/initial collision between two different
    people effectively impossible, so it stays low-risk.
    """
    ta, tb = _norm_ordered(a), _norm_ordered(b)
    if not ta or not tb or ta[-1] != tb[-1]:
        return False
    ga, gb = ta[:-1], tb[:-1]            # given-name tokens
    if not ga or not gb:
        return True                      # only a surname present on one side
    return ga[0][0] == gb[0][0]


def _res(springer_name, dblp_name, orcid, ordinal, method):
    return {"springer_name": springer_name, "dblp_name": dblp_name,
            "orcid": orcid, "ordinal": ordinal, "match_method": method}


def match_doi(springer_authors, dblp_sigs):
    """Align one paper's author lists with tiered matching.

    springer_authors: list[dict] with keys name, position (position may be falsy)
    dblp_sigs:        list[dict] with keys name, ordinal, orcid (orcid/ordinal may be falsy)
    Returns one result dict per springer author. Tiers, most→least confident:
      1. unique exact normalized-name match
      2. exact name ambiguous -> resolved by equal position (ordinal)
      3. no exact match -> position + loose name (surname + first initial) [_loose]
    """
    exact_index = defaultdict(list)
    by_ordinal = {}
    for sig in dblp_sigs:
        exact_index[norm_tokens(sig["name"])].append(sig)
        o = _to_int(sig.get("ordinal"))
        if o is not None:
            by_ordinal[o] = sig

    results = []
    for a in springer_authors:
        sname = a["name"]
        pos = _to_int(a.get("position"))
        key = norm_tokens(sname)
        cands = exact_index.get(key, []) if key else []
        sig, loose = None, False

        if len(cands) == 1:
            sig = cands[0]
        elif len(cands) > 1:                                  # resolve by position
            pick = [c for c in cands if _to_int(c.get("ordinal")) == pos]
            if len(pick) == 1:
                sig = pick[0]
            else:
                results.append(_res(sname, cands[0]["name"], None, None, "ambiguous_match"))
                continue
        elif pos is not None and pos in by_ordinal and loose_compatible(sname, by_ordinal[pos]["name"]):
            sig, loose = by_ordinal[pos], True

        if sig is None:
            results.append(_res(sname, None, None, None, "no_dblp_match"))
            continue
        method = "matched_orcid" if sig.get("orcid") else "matched_no_orcid"
        if loose:
            method += "_loose"
        results.append(_res(sname, sig["name"], sig.get("orcid") or None,
                            sig.get("ordinal"), method))
    return results


# ---------------------------------------------------------------------------
# I/O helpers
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


def load_dblp_tsv(path):
    """doi -> list of {name, ordinal, orcid}."""
    by_doi = defaultdict(list)
    with open(path, encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            by_doi[row["doi"]].append({
                "name": row.get("dblp_name") or "",
                "ordinal": row.get("ordinal") or None,
                "orcid": (row.get("orcid") or "").strip() or None,
            })
    return by_doi


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Match DBLP ORCIDs to springer authors.")
    parser.add_argument("--tsv", default="dblp_orcid_raw.tsv",
                        help="DBLP projection from dblp_orcid_extract.py")
    parser.add_argument("--selftest", action="store_true", help="run matching validation and exit")
    args = parser.parse_args()

    if args.selftest:
        return selftest()

    from datetime import datetime
    from database import Postgress

    if not os.path.exists(args.tsv):
        raise SystemExit(f"{args.tsv} not found; run dblp_orcid_extract.py first.")
    dblp_by_doi = load_dblp_tsv(args.tsv)
    print(f"Loaded DBLP signatures for {len(dblp_by_doi)} DOIs from {args.tsv}")

    config = read_config(ENV_PATH)
    db = Postgress(server=config["POSTGRES_SERVER"], database=config["POSTGRES_DB"],
                   user=config["POSTGRES_USER"], password=config["POSTGRES_PASSWORD"])

    rows = db.execute_query_result(f'SELECT doi, name, position FROM "{SCHEMA}"."authors"')
    springer_by_doi = defaultdict(list)
    for r in rows:
        springer_by_doi[r["doi"]].append({"name": r["name"], "position": r["position"]})
    print(f"Loaded {len(rows)} springer author rows across {len(springer_by_doi)} DOIs")

    now = datetime.now().isoformat(timespec="seconds")
    out_rows = []
    counts = defaultdict(int)
    dois_with_dblp = 0
    for doi, sauthors in springer_by_doi.items():
        sigs = dblp_by_doi.get(doi, [])
        if sigs:
            dois_with_dblp += 1
        for res in match_doi(sauthors, sigs):
            counts[res["match_method"]] += 1
            out_rows.append({"doi": doi, "springer_name": res["springer_name"],
                             "dblp_name": res["dblp_name"], "orcid": res["orcid"],
                             "ordinal": res["ordinal"], "match_method": res["match_method"],
                             "retrieved_at": now})

    # --- (re)build the mapping table --------------------------------------
    db.execute_query(f'DROP TABLE IF EXISTS "{SCHEMA}"."{OUT_TABLE}"')
    db.create_table(SCHEMA, OUT_TABLE, {c: 0 for c in
                    ["doi", "springer_name", "dblp_name", "orcid", "ordinal",
                     "match_method", "retrieved_at"]})
    BATCH = 5000
    for i in range(0, len(out_rows), BATCH):
        db.insert_into(SCHEMA, OUT_TABLE, out_rows[i:i + BATCH])

    # --- coverage report ---------------------------------------------------
    total = len(out_rows)
    orcid_exact = counts["matched_orcid"]
    orcid_loose = counts["matched_orcid_loose"]
    print(f"\nWrote {SCHEMA}.{OUT_TABLE}: {total} author rows")
    print(f"  springer DOIs with a DBLP match: {dois_with_dblp}/{len(springer_by_doi)} "
          f"({dois_with_dblp/max(len(springer_by_doi),1):.1%})")
    for m in ("matched_orcid", "matched_orcid_loose", "matched_no_orcid",
              "matched_no_orcid_loose", "ambiguous_match", "no_dblp_match"):
        print(f"  {m:<22} {counts[m]:>8}  ({counts[m]/max(total,1):.1%})")
    print(f"\nORCID coverage of author rows: {orcid_exact + orcid_loose}/{total} "
          f"({(orcid_exact + orcid_loose)/max(total,1):.1%})  "
          f"[exact {orcid_exact}, loose {orcid_loose}]")
    print(f"Distinct ORCIDs assigned: {len({r['orcid'] for r in out_rows if r['orcid']})}")


# ---------------------------------------------------------------------------
# Self-test (no DB)
# ---------------------------------------------------------------------------

def selftest():
    ok = True
    def check(name, cond):
        nonlocal ok
        print(f"  [{'PASS' if cond else 'FAIL'}] {name}")
        ok = ok and cond

    # normalization
    check("diacritics folded: 'José Andrade' == 'Jose Andrade'",
          norm_tokens("José Andrade") == norm_tokens("Jose Andrade"))
    check("token order ignored: 'Zeyd Boukhers' == 'Boukhers Zeyd'",
          norm_tokens("Zeyd Boukhers") == norm_tokens("Boukhers, Zeyd"))
    check("DBLP homonym suffix stripped: 'Wei Wang 0001' == 'Wei Wang'",
          norm_tokens("Wei Wang 0001") == norm_tokens("Wei Wang"))
    check("distinct names differ", norm_tokens("Wei Wang") != norm_tokens("Wei Li"))

    sigs = [
        {"name": "Zeyd Boukhers", "ordinal": "1", "orcid": "0000-0001-1234-5678"},
        {"name": "Nagaraj Bahubali Asundi", "ordinal": "2", "orcid": None},
        {"name": "Wei Wang 0001", "ordinal": "3", "orcid": "0000-0002-0000-0000"},
    ]
    authors = [{"name": "Boukhers, Zeyd", "position": "1"},
               {"name": "Nagaraj Bahubali Asundi", "position": "2"},
               {"name": "Wei Wang", "position": "3"},
               {"name": "Unknom Author", "position": "4"}]
    res = {r["springer_name"]: r for r in match_doi(authors, sigs)}

    check("exact/reordered match yields ORCID",
          res["Boukhers, Zeyd"]["match_method"] == "matched_orcid"
          and res["Boukhers, Zeyd"]["orcid"] == "0000-0001-1234-5678")
    check("DBLP match without ORCID -> matched_no_orcid",
          res["Nagaraj Bahubali Asundi"]["match_method"] == "matched_no_orcid"
          and res["Nagaraj Bahubali Asundi"]["orcid"] is None)
    check("homonym-suffixed DBLP name still matches + ORCID",
          res["Wei Wang"]["match_method"] == "matched_orcid"
          and res["Wei Wang"]["orcid"] == "0000-0002-0000-0000")
    check("springer author absent from DBLP -> no_dblp_match",
          res["Unknom Author"]["match_method"] == "no_dblp_match")

    # position + loose name: initials vs full name, same surname & first initial & position
    loose = match_doi([{"name": "Z. Boukhers", "position": "1"}], sigs)
    check("position + loose (initial) -> matched_orcid_loose",
          loose[0]["match_method"] == "matched_orcid_loose"
          and loose[0]["orcid"] == "0000-0001-1234-5678")

    # loose must FAIL when the first initial differs, even at the same position
    bad = match_doi([{"name": "Q. Boukhers", "position": "1"}], sigs)
    check("loose rejects mismatched initial -> no_dblp_match",
          bad[0]["match_method"] == "no_dblp_match")

    # ambiguous exact key resolved by position (ordinal)
    twins = [{"name": "Jun Yu 0001", "ordinal": "1", "orcid": "x"},
             {"name": "Jun Yu 0002", "ordinal": "2", "orcid": "y"}]
    pick = match_doi([{"name": "Jun Yu", "position": "2"}], twins)
    check("ambiguous exact resolved by position -> ORCID of ordinal 2",
          pick[0]["match_method"] == "matched_orcid" and pick[0]["orcid"] == "y")

    # ambiguous exact, NO position to resolve -> ambiguous_match, no ORCID guessed
    amb = match_doi([{"name": "Jun Yu", "position": None}], twins)
    check("ambiguous same-name, no position -> ambiguous_match, no ORCID",
          amb[0]["match_method"] == "ambiguous_match" and amb[0]["orcid"] is None)

    print("\nSELFTEST:", "ALL PASS" if ok else "FAILURES PRESENT")
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
