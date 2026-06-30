"""
Matching authors based on ORCIDs instead of names -- step 1 of 2: project (doi, name, ordinal, orcid)
out of the local DBLP RDF store for the Springer DOIs we care about.

DBLP RDF model (verified against https://dblp.org/rdf/docu/):
  ?pub dblp:doi ?doi ; dblp:hasSignature ?sig .
  ?sig dblp:signatureDblpName ?name ;          # name as printed on this paper
       dblp:signatureOrdinal  ?ordinal ;       # author position (1-based)
       dblp:signatureOrcid    ?orcid ;         # ORCID for this authorship (optional)
       dblp:signatureCreator  ?person .        # the dblp Person (?person dblp:orcid ...)
We take ORCID from the signature, falling back to the Person's ORCID.

This script does not touch PostgreSQL. It reads the Springer DOIs from a plain
text file (one DOI per line) that you export once:

    \\copy (SELECT DISTINCT doi FROM springer.articles) TO 'springer_dois.txt'

Local store: pyoxigraph (embedded, pip install pyoxigraph) by default; or point
--endpoint at any SPARQL HTTP endpoint (Fuseki/Virtuoso/live dblp). The SPARQL is
identical either way.

Typical use:
    # one-time: bulk-load the DBLP N-Triples dump into an on-disk oxigraph store
    # (dblp.nt.gz from dblp.org/rdf; loaded directly, no need to gunzip)
    python dblp_orcid_extract.py --store ./dblp_store --load dblp.nt.gz
    # then project for our DOIs
    python dblp_orcid_extract.py --store ./dblp_store --dois springer_dois.txt --out dblp_orcid_raw.tsv
    # or against an HTTP endpoint instead of a local store:
    python dblp_orcid_extract.py --endpoint http://localhost:3030/dblp/sparql --dois springer_dois.txt

    python dblp_orcid_extract.py --selftest    # validates query/term construction, no store
"""

from __future__ import annotations

import argparse
import csv
import os
import re

DBLP_PREFIX = "PREFIX dblp: <https://dblp.org/rdf/schema#>"

_ORCID_RE = re.compile(r"(\d{4}-\d{4}-\d{4}-\d{3}[\dxX])")


# ---------------------------------------------------------------------------
# Pure helpers (unit-tested via --selftest)
# ---------------------------------------------------------------------------

def _esc_literal(s):
    return s.replace("\\", "\\\\").replace('"', '\\"')


def doi_term(doi):
    """SPARQL term to match against dblp:doi. DBLP stores the DOI as an IRI
    https://doi.org/<DOI> with the suffix uppercased (confirmed via --debug; the
    bare/literal/lowercase variants returned 0 matches)."""
    return f"<https://doi.org/{doi.upper()}>"


def build_query(dois):
    """SELECT projecting (sdoi, name, ordinal, orcid) for a batch of DOIs.

    ?sdoi is the original Springer DOI (kept so results map back); ?doi is the
    uppercased doi.org IRI actually matched against dblp:doi.
    """
    pairs = "\n    ".join(f'("{_esc_literal(d)}" {doi_term(d)})' for d in dois)
    return f"""{DBLP_PREFIX}
SELECT ?sdoi ?name ?ordinal ?orcid WHERE {{
  VALUES (?sdoi ?doi) {{
    {pairs}
  }}
  ?pub dblp:doi ?doi ;
       dblp:hasSignature ?sig .
  ?sig dblp:signatureDblpName ?name .
  OPTIONAL {{ ?sig dblp:signatureOrdinal ?ordinal }}
  OPTIONAL {{ ?sig dblp:signatureOrcid ?sorcid }}
  OPTIONAL {{ ?sig dblp:signatureCreator ?person . OPTIONAL {{ ?person dblp:orcid ?corcid }} }}
  BIND(COALESCE(?sorcid, ?corcid) AS ?orcid)
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


# ---------------------------------------------------------------------------
# Store backends -- both expose run_query(sparql) -> list[dict[str, str|None]]
# ---------------------------------------------------------------------------

class OxigraphStore:
    def __init__(self, path):
        from pyoxigraph import Store
        self.store = Store(path)

    def load(self, dump_path):
        import gzip
        from pyoxigraph import RdfFormat
        base = dump_path[:-3] if dump_path.endswith(".gz") else dump_path
        if base.endswith(".nt"):
            fmt = RdfFormat.N_TRIPLES
        elif base.endswith(".ttl"):
            fmt = RdfFormat.TURTLE
        else:
            fmt = RdfFormat.RDF_XML
        print(f"Bulk-loading {dump_path} ({fmt}) into the store -- can take 10-30+ min "
              f"and tens of GB of disk ...")
        # pyoxigraph does not auto-decompress .gz, so stream-decompress it here and
        # feed the decompressed bytes to bulk_load (no giant uncompressed file on disk).
        if dump_path.endswith(".gz"):
            with gzip.open(dump_path, "rb") as f:
                self.store.bulk_load(f, format=fmt)
        else:
            self.store.bulk_load(path=dump_path, format=fmt)
        print("Load complete.")

    def run_query(self, sparql):
        out = []
        for sol in self.store.query(sparql):
            out.append({v: (sol[v].value if sol[v] is not None else None)
                        for v in ("sdoi", "name", "ordinal", "orcid")})
        return out

    def run_raw(self, sparql):
        res = self.store.query(sparql)
        names = [str(v).lstrip("?") for v in res.variables]
        out = []
        for sol in res:
            out.append({n: (sol[i].value if sol[i] is not None else None)
                        for i, n in enumerate(names)})
        return out


class HttpStore:
    def __init__(self, endpoint):
        self.endpoint = endpoint

    def load(self, dump_path):
        raise SystemExit("--load is only for --store (oxigraph); load the dump into your endpoint separately.")

    def run_query(self, sparql):
        import json
        import urllib.parse
        import urllib.request
        data = urllib.parse.urlencode({"query": sparql}).encode()
        req = urllib.request.Request(
            self.endpoint, data=data,
            headers={"Accept": "application/sparql-results+json"})
        with urllib.request.urlopen(req, timeout=120) as resp:
            res = json.load(resp)
        out = []
        for b in res.get("results", {}).get("bindings", []):
            out.append({v: (b[v]["value"] if v in b else None)
                        for v in ("sdoi", "name", "ordinal", "orcid")})
        return out

    def run_raw(self, sparql):
        import json
        import urllib.parse
        import urllib.request
        data = urllib.parse.urlencode({"query": sparql}).encode()
        req = urllib.request.Request(self.endpoint, data=data,
                                     headers={"Accept": "application/sparql-results+json"})
        with urllib.request.urlopen(req, timeout=120) as resp:
            res = json.load(resp)
        vars_ = res.get("head", {}).get("vars", [])
        return [{v: (b[v]["value"] if v in b else None) for v in vars_}
                for b in res.get("results", {}).get("bindings", [])]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def debug(store, sample_doi="10.1007/s00799-023-00361-6"):
    """Inspect the store: is it loaded, what namespace, how is dblp:doi serialized?"""
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

    show("A. store non-empty? (any 3 triples)",
         "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 3")
    show("B. dblp:doi sample values (is the predicate/namespace right + value format)",
         "PREFIX dblp: <https://dblp.org/rdf/schema#> "
         "SELECT ?o WHERE { ?s dblp:doi ?o } LIMIT 10")
    show("C. distinct predicates (what namespace is actually used)",
         "SELECT DISTINCT ?p WHERE { ?s ?p ?o } LIMIT 50")


def verify_doi_matches(store, sample):
    """Fail fast if the DOI serialization doesn't match (empty store / format drift)."""
    n = len(store.run_query(build_query(sample)))
    if n == 0:
        raise SystemExit(
            "No DBLP matches on the sample -- is the store loaded and do these DOIs "
            "exist in DBLP? Inspect with:  --store <path> --debug")
    print(f"DOI match confirmed on sample: {n} signature rows from {len(sample)} DOIs.")


def main():
    ap = argparse.ArgumentParser(description="Project DBLP ORCIDs for Springer DOIs.")
    ap.add_argument("--store", help="path to an on-disk pyoxigraph store")
    ap.add_argument("--endpoint", help="SPARQL HTTP endpoint URL (instead of --store)")
    ap.add_argument("--load", help="bulk-load this RDF dump into --store, then exit")
    ap.add_argument("--dois", default="springer_dois.txt", help="one DOI per line")
    ap.add_argument("--out", default="dblp_orcid_raw.tsv")
    ap.add_argument("--batch", type=int, default=200, help="DOIs per SPARQL VALUES block")
    ap.add_argument("--debug", action="store_true", help="inspect the store (diagnose 0-match)")
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args()

    if args.selftest:
        return selftest()

    if not args.store and not args.endpoint:
        raise SystemExit("Provide --store <path> (pyoxigraph) or --endpoint <url>.")
    store = OxigraphStore(args.store) if args.store else HttpStore(args.endpoint)

    if args.load:
        return store.load(args.load)

    if args.debug:
        return debug(store)

    if not os.path.exists(args.dois):
        raise SystemExit(f"{args.dois} not found. Export it from DBeaver (SELECT DISTINCT "
                         f"doi FROM springer.articles -> Export resultset -> CSV, no header).")
    with open(args.dois, encoding="utf-8-sig") as f:        # -sig strips a BOM if present
        dois = []
        for ln in f:
            d = ln.strip().strip('"').strip("'").strip()    # tolerate quotes/whitespace
            if d and d.lower() != "doi":                    # skip a stray header row
                dois.append(d)
    print(f"{len(dois)} Springer DOIs to look up.")

    verify_doi_matches(store, dois[:200])

    n_rows = 0
    seen_doi = set()
    with open(args.out, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["doi", "dblp_name", "ordinal", "orcid"])
        for bi, batch in enumerate(batched(dois, args.batch), 1):
            for r in store.run_query(build_query(batch)):
                seen_doi.add(r["sdoi"])
                w.writerow([r["sdoi"], r["name"] or "", r["ordinal"] or "",
                            norm_orcid(r["orcid"]) or ""])
                n_rows += 1
            if bi % 20 == 0:
                print(f"  ...{bi} batches, {n_rows} signature rows so far")
    matched_dois = len(seen_doi)
    print(f"\nWrote {args.out}: {n_rows} signature rows for "
          f"{matched_dois}/{len(dois)} DOIs ({matched_dois/max(len(dois),1):.1%} found in DBLP).")


# ---------------------------------------------------------------------------
# Self-test (pure string/format logic; no store)
# ---------------------------------------------------------------------------

def selftest():
    ok = True
    def check(name, cond):
        nonlocal ok
        print(f"  [{'PASS' if cond else 'FAIL'}] {name}")
        ok = ok and cond

    check("doi_term is an uppercased doi.org IRI",
          doi_term("10.1007/s00799-abc") == "<https://doi.org/10.1007/S00799-ABC>")
    check("literal escaping helper", _esc_literal('a"b') == 'a\\"b')

    check("orcid from bare", norm_orcid("0000-0002-1825-0097") == "0000-0002-1825-0097")
    check("orcid from IRI",
          norm_orcid("https://orcid.org/0000-0002-1825-0097") == "0000-0002-1825-0097")
    check("orcid X checksum kept", norm_orcid("0000-0002-1694-233X") == "0000-0002-1694-233X")
    check("no orcid -> None", norm_orcid("") is None and norm_orcid(None) is None)

    q = build_query(["10.1007/a", "10.1007/b"])
    check("query has VALUES pairs mapping sdoi -> uppercased IRI",
          '("10.1007/a" <https://doi.org/10.1007/A>)' in q
          and '("10.1007/b" <https://doi.org/10.1007/B>)' in q)
    check("query selects the 4 projection vars",
          "SELECT ?sdoi ?name ?ordinal ?orcid" in q)
    check("query uses signature + COALESCE for orcid",
          "dblp:hasSignature" in q and "COALESCE(?sorcid, ?corcid)" in q)

    check("batched splits correctly",
          list(batched([1, 2, 3, 4, 5], 2)) == [[1, 2], [3, 4], [5]])

    print("\nSELFTEST:", "ALL PASS" if ok else "FAILURES PRESENT")
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
