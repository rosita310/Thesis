"""
ORCID-based author matching, step 1 of 2: project (doi, name, ordinal, orcid) out
of the local DBLP store for our Springer DOIs.

A DBLP ORCID sits on the signature, or failing that on the Person it creates.
Reaching both needs a nested OPTIONAL, which fans out where a Person carries
several dblp:orcid values: the same authorship returns once per candidate.
resolve_signatures() collapses that client-side and assigns an ORCID only when it
is unambiguous -- the ORCID is the BBN's identity key, so a wrong one merges or
splits an author and shifts n_gaps.

    python dblp_orcid_extract.py --store ./dblp_store --load
    python dblp_orcid_extract.py --store ./dblp_store --dois springer_dois.txt --out dblp_orcid_raw.tsv
    python dblp_orcid_extract.py --selftest

springer_dois.txt is one DOI per line, exported once:
    \\copy (SELECT DISTINCT doi FROM springer.articles) TO 'springer_dois.txt'
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import time
from collections import defaultdict
from pathlib import Path

DBLP_PREFIX = "PREFIX dblp: <https://dblp.org/rdf/schema#>"

_ORCID_RE = re.compile(r"(\d{4}-\d{4}-\d{4}-\d{3}[\dxX])")

SOLUTION_DIR = Path(__file__).resolve().parents[3]
DUMP_NAME = "dblp.nt"


def dblp_dump():
    """Unpacking dblp.nt.gz leaves the file either in solution/ or in a folder of
    the same name, so accept both."""
    p = SOLUTION_DIR / DUMP_NAME
    if p.is_dir():
        p = p / DUMP_NAME
    if not p.is_file():
        raise SystemExit(f"{p} not found -- unpack dblp.nt.gz (gunzip / 7z x) into "
                         f"{SOLUTION_DIR}.")
    return str(p)


# --- Pure helpers (unit-tested via --selftest) ------------------------------

def _esc_literal(s):
    return s.replace("\\", "\\\\").replace('"', '\\"')


def doi_term(doi):
    """DBLP stores the DOI as an uppercased doi.org IRI; other forms match nothing."""
    return f"<https://doi.org/{doi.upper()}>"


def build_query(dois):
    """?sdoi carries the original Springer DOI so results map back. The two ORCID
    sources stay separate instead of being COALESCEd, so resolve_signatures() can
    see them disagree. One query, not two: a second pass for the Person fallback
    measured slower, as it re-walks the same publication->signature join."""
    pairs = "\n    ".join(f'("{_esc_literal(d)}" {doi_term(d)})' for d in dois)
    return f"""{DBLP_PREFIX}
SELECT ?sdoi ?name ?ordinal ?sorcid ?corcid WHERE {{
  VALUES (?sdoi ?doi) {{
    {pairs}
  }}
  ?pub dblp:doi ?doi ;
       dblp:hasSignature ?sig .
  ?sig dblp:signatureDblpName ?name .
  OPTIONAL {{ ?sig dblp:signatureOrdinal ?ordinal }}
  OPTIONAL {{ ?sig dblp:signatureOrcid ?sorcid }}
  OPTIONAL {{ ?sig dblp:signatureCreator ?person . OPTIONAL {{ ?person dblp:orcid ?corcid }} }}
}}"""


def norm_orcid(value):
    """Reduce an ORCID literal/IRI to the bare 0000-0000-0000-000X form, or None."""
    if not value:
        return None
    m = _ORCID_RE.search(str(value))
    return m.group(1).upper() if m else None


def batched(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


# --- Store ------------------------------------------------------------------

class OxigraphStore:
    def __init__(self, path, read_only=False):
        from pyoxigraph import Store
        if read_only:
            if not hasattr(Store, "read_only"):
                raise SystemExit("--read-only needs pyoxigraph >= 0.4.")
            self.store = Store.read_only(path)
        else:
            self.store = Store(path)

    def load(self):
        from pyoxigraph import RdfFormat
        path = dblp_dump()
        print(f"Bulk-loading {path} -- can take 10-30+ min and tens of GB of disk.")
        t0 = time.perf_counter()
        self.store.bulk_load(path=path, format=RdfFormat.N_TRIPLES)
        print(f"Load complete in {time.perf_counter() - t0:.0f} s.")

    def run_raw(self, sparql):
        res = self.store.query(sparql)
        names = [str(v).lstrip("?") for v in res.variables]
        return [{n: (sol[i].value if sol[i] is not None else None)
                 for i, n in enumerate(names)} for sol in res]


# --- Projection -------------------------------------------------------------

def _sig_key(row):
    """Not the signature node itself: DBLP models signatures as blank nodes,
    whose labels are not stable references."""
    return (row.get("sdoi"), row.get("name"), row.get("ordinal"))


def resolve_signatures(rows, stats=None):
    """One row per signature, keeping an ORCID only where the candidates agree.
    Order is preserved; pass a dict as `stats` for the per-source counts."""
    own, person, order = defaultdict(set), defaultdict(set), []
    for r in rows:
        k = _sig_key(r)
        if k not in own:
            order.append(k)
            own[k]                      # touch, so a no-ORCID signature still exists
        o = norm_orcid(r.get("sorcid"))
        if o:
            own[k].add(o)
        c = norm_orcid(r.get("corcid"))
        if c:
            person[k].add(c)

    st = stats if stats is not None else {}
    out = []
    for k in order:
        sdoi, name, ordinal = k
        o_own, o_per = own[k], person[k]
        if len(o_own) == 1:
            orcid, src = next(iter(o_own)), "own"
        elif len(o_own) > 1:
            orcid, src = None, "own_ambiguous"
        elif len(o_per) == 1:
            orcid, src = next(iter(o_per)), "person"
        elif len(o_per) > 1:
            orcid, src = None, "person_ambiguous"
        else:
            orcid, src = None, "none"
        st[src] = st.get(src, 0) + 1
        out.append({"sdoi": sdoi, "name": name, "ordinal": ordinal, "orcid": orcid})
    return out


def raw_rows(rows):
    """The historical projection, fan-out kept. Only for --no-dedupe."""
    return [{"sdoi": r.get("sdoi"), "name": r.get("name"),
             "ordinal": r.get("ordinal"),
             "orcid": norm_orcid(r.get("sorcid")) or norm_orcid(r.get("corcid"))}
            for r in rows]


def read_dois(path):
    """One DOI per line; tolerates a BOM, quotes, blank lines and a header row."""
    if not os.path.exists(path):
        raise SystemExit(f"{path} not found. Export it from DBeaver (SELECT DISTINCT "
                         f"doi FROM springer.articles -> Export resultset -> CSV, no header).")
    dois = []
    with open(path, encoding="utf-8-sig") as f:
        for ln in f:
            d = ln.strip().strip('"').strip("'").strip()
            if d and d.lower() != "doi":
                dois.append(d)
    return dois


# --- Commands ---------------------------------------------------------------

def debug(store):
    """Is the store loaded, what namespace, how is dblp:doi serialized?"""
    def show(label, q):
        print(f"\n--- {label} ---")
        try:
            rows = store.run_raw(q)
        except Exception as e:                       # noqa: BLE001
            print(f"  query error: {e}")
            return
        if not rows:
            print("  (no rows)")
        for r in rows[:15]:
            print("  " + " | ".join(f"{k}={v}" for k, v in r.items()))

    show("A. store non-empty?", "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 3")
    show("B. dblp:doi sample values",
         "PREFIX dblp: <https://dblp.org/rdf/schema#> "
         "SELECT ?o WHERE { ?s dblp:doi ?o } LIMIT 10")
    show("C. distinct predicates",
         "SELECT DISTINCT ?p WHERE { ?s ?p ?o } LIMIT 50")


def audit_orcids(store, args):
    """Report the ORCID fan-out over the first N DOIs. Writes nothing."""
    dois = read_dois(args.dois)[:args.audit]
    print(f"Auditing {len(dois)} DOIs (batch={args.batch}) ...")

    t0 = time.perf_counter()
    n_raw = n_sig = 0
    stats = {}
    worst = []
    for batch in batched(dois, args.batch):
        rows = store.run_raw(build_query(batch))
        n_raw += len(rows)
        n_sig += len(resolve_signatures(rows, stats))
        cand = defaultdict(set)
        for r in rows:
            for key in ("sorcid", "corcid"):
                o = norm_orcid(r.get(key))
                if o:
                    cand[_sig_key(r)].add(o)
        worst.extend((len(v), k) for k, v in cand.items() if len(v) > 1)
    elapsed = time.perf_counter() - t0

    amb = stats.get("own_ambiguous", 0) + stats.get("person_ambiguous", 0)
    print(f"\n  elapsed             : {elapsed:.2f} s")
    print(f"  raw rows            : {n_raw}")
    print(f"  distinct signatures : {n_sig}")
    print(f"  duplicate rows      : {n_raw - n_sig} "
          f"({100 * (n_raw - n_sig) / max(n_raw, 1):.2f}% of raw rows)")
    print("\n  ORCID source breakdown (per signature):")
    print(f"    from the signature  : {stats.get('own', 0)}")
    print(f"    from creator Person : {stats.get('person', 0)}")
    print(f"    none available      : {stats.get('none', 0)}")
    print(f"    SUPPRESSED ambiguous: {amb} "
          f"({100 * amb / max(n_sig, 1):.2f}% of signatures)")
    if worst:
        worst.sort(key=lambda t: t[0], reverse=True)   # keys may hold None; sort on the count only
        print("\n  worst offenders (candidate ORCIDs for one authorship):")
        for n, (doi, name, ordinal) in worst[:10]:
            print(f"    {n} ORCIDs  {doi} | {name} | ord {ordinal}")


def verify_doi_matches(store, sample):
    """Fail fast on an empty store or a changed DOI serialization."""
    n = len(store.run_raw(build_query(sample)))
    if n == 0:
        raise SystemExit("No DBLP matches on the sample -- is the store loaded? "
                         "Inspect with:  --store <path> --debug")
    print(f"DOI match confirmed on sample: {n} signature rows from {len(sample)} DOIs.")


def extract(store, args):
    dois = read_dois(args.dois)
    print(f"{len(dois)} Springer DOIs to look up.")
    verify_doi_matches(store, dois[:args.batch])

    t0 = time.perf_counter()
    n_rows = n_orcid = 0
    stats = {}
    seen_doi = set()
    n_batches = (len(dois) + args.batch - 1) // args.batch
    mode = "raw fan-out (historical)" if args.no_dedupe else "one row per signature"
    print(f"Extraction, {n_batches} batches of {args.batch} DOIs -- {mode}.")
    with open(args.out, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["doi", "dblp_name", "ordinal", "orcid"])
        for bi, batch in enumerate(batched(dois, args.batch), 1):
            rows = store.run_raw(build_query(batch))
            for r in (raw_rows(rows) if args.no_dedupe else resolve_signatures(rows, stats)):
                seen_doi.add(r["sdoi"])
                orcid = r["orcid"] or ""
                w.writerow([r["sdoi"], r["name"] or "", r["ordinal"] or "", orcid])
                n_rows += 1
                n_orcid += bool(orcid)
            if bi % 20 == 0:
                print(f"  ...{bi}/{n_batches} batches, {n_rows} signature rows, "
                      f"{n_orcid} with an ORCID")

    matched = len(seen_doi)
    print(f"\nElapsed: {time.perf_counter() - t0:.1f} s "
          f"({len(dois)} DOIs, batch={args.batch}, {mode}).")
    print(f"Wrote {args.out}: {n_rows} signature rows for "
          f"{matched}/{len(dois)} DOIs ({matched / max(len(dois), 1):.1%} found in DBLP); "
          f"{n_orcid} rows carry an ORCID ({n_orcid / max(n_rows, 1):.1%}).")
    if stats:
        amb = stats.get("own_ambiguous", 0) + stats.get("person_ambiguous", 0)
        print(f"  ORCID source: {stats.get('own', 0)} from the signature, "
              f"{stats.get('person', 0)} from the creator Person, "
              f"{stats.get('none', 0)} none available, "
              f"{amb} SUPPRESSED as ambiguous "
              f"({stats.get('own_ambiguous', 0)} signature / "
              f"{stats.get('person_ambiguous', 0)} Person carried >1 ORCID).")


def main():
    ap = argparse.ArgumentParser(description="Project DBLP ORCIDs for Springer DOIs.")
    ap.add_argument("--store", help="path to an on-disk pyoxigraph store")
    ap.add_argument("--load", action="store_true",
                    help=f"bulk-load {DUMP_NAME} from {SOLUTION_DIR} into --store, then exit")
    ap.add_argument("--dois", default="springer_dois.txt", help="one DOI per line")
    ap.add_argument("--out", default="dblp_orcid_raw.tsv")
    ap.add_argument("--batch", type=int, default=200, help="DOIs per SPARQL VALUES block")
    ap.add_argument("--read-only", action="store_true",
                    help="open --store without the write lock (pyoxigraph >= 0.4)")
    ap.add_argument("--audit", type=int, metavar="N",
                    help="report the ORCID fan-out over the first N DOIs; writes nothing")
    ap.add_argument("--no-dedupe", action="store_true",
                    help="reproduce the historical raw output, fan-out and all")
    ap.add_argument("--debug", action="store_true", help="inspect the store (diagnose 0-match)")
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args()

    if args.selftest:
        return selftest()

    if not args.store:
        raise SystemExit("Provide --store <path>.")
    store = OxigraphStore(args.store, read_only=args.read_only)

    if args.load:
        return store.load()
    if args.debug:
        return debug(store)
    if args.audit is not None:
        return audit_orcids(store, args)
    return extract(store, args)


# --- Self-test (pure logic; no store) ---------------------------------------

def selftest():
    ok = True
    def check(name, cond):
        nonlocal ok
        print(f"  [{'PASS' if cond else 'FAIL'}] {name}")
        ok = ok and cond

    def row(doi, name, ordinal, sorcid=None, corcid=None):
        return {"sdoi": doi, "name": name, "ordinal": ordinal,
                "sorcid": sorcid, "corcid": corcid}

    def orcids(rows, stats=None):
        return [r["orcid"] for r in resolve_signatures(rows, stats)]

    check("doi_term uppercases into a doi.org IRI",
          doi_term("10.1007/s00799-abc") == "<https://doi.org/10.1007/S00799-ABC>")
    check("quotes in a DOI stay escaped", _esc_literal('a"b') == 'a\\"b')
    check("norm_orcid strips the IRI, keeps the X checksum, else None",
          norm_orcid("https://orcid.org/0000-0002-1825-0097") == "0000-0002-1825-0097"
          and norm_orcid("0000-0002-1694-233x") == "0000-0002-1694-233X"
          and norm_orcid("") is None and norm_orcid(None) is None)
    check("batched splits correctly",
          list(batched([1, 2, 3, 4, 5], 2)) == [[1, 2], [3, 4], [5]])

    q = build_query(["10.1007/a", "10.1007/b"])
    check("VALUES maps each raw DOI to its uppercased IRI",
          '("10.1007/a" <https://doi.org/10.1007/A>)' in q
          and '("10.1007/b" <https://doi.org/10.1007/B>)' in q)
    check("both ORCID sources projected separately, no COALESCE",
          "SELECT ?sdoi ?name ?ordinal ?sorcid ?corcid" in q and "COALESCE" not in q)

    check("SOLUTION_DIR points at solution/", SOLUTION_DIR.name == "solution")
    try:
        print(f"  [ -- ] dump: {dblp_dump()}")
    except SystemExit as e:
        print(f"  [WARN] {e}")

    st = {}
    check("signature ORCID, else Person ORCID, else none -- one row each, in order",
          orcids([row("10.1/a", "A One", "1", sorcid="0000-0001-2345-6789"),
                  row("10.1/a", "B Two", "2", corcid="https://orcid.org/0000-0002-1825-0097"),
                  row("10.1/a", "C Three", "3")], st)
          == ["0000-0001-2345-6789", "0000-0002-1825-0097", None]
          and (st.get("own"), st.get("person"), st.get("none")) == (1, 1, 1))

    st = {}
    check("a Person carrying several ORCIDs -> one row, ORCID suppressed",
          orcids([row("10.1/c", "E Five", "5", corcid=f"0000-0002-0000-000{i}")
                  for i in (1, 2, 3)], st) == [None]
          and st.get("person_ambiguous") == 1)

    st = {}
    check("a signature carrying several ORCIDs -> one row, ORCID suppressed",
          orcids([row("10.1/d", "F Six", "1", sorcid=f"0000-0002-1111-000{i}")
                  for i in (1, 2)], st) == [None]
          and st.get("own_ambiguous") == 1)

    check("repeated rows agreeing on one ORCID collapse and keep it",
          orcids([row("10.1/e", "G Seven", "1", sorcid="0000-0002-2222-3333")] * 2)
          == ["0000-0002-2222-3333"])

    check("the signature's own ORCID wins over an ambiguous Person",
          orcids([row("10.1/f", "H", "1", "0000-0003-4444-5555", "0000-0003-9999-0001"),
                  row("10.1/f", "H", "1", "0000-0003-4444-5555", "0000-0003-9999-0002")])
          == ["0000-0003-4444-5555"])

    check("--no-dedupe keeps one row per candidate ORCID",
          len(raw_rows([row("10.1/c", "E Five", "5", corcid=f"0000-0002-0000-000{i}")
                        for i in (1, 2, 3)])) == 3)

    print("\nSELFTEST:", "ALL PASS" if ok else "FAILURES PRESENT")
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
