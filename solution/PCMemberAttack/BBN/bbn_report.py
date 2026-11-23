"""
Sensitivity analysis of the PC member citation BBN: the shortlist under every variant of
SENSITIVITY_VARIANTS, one setting changed at a time, against the shortlist of the run.

A shortlist is the gated members (Westerbaan's gate) with P(genuine) below
SHORTLIST_THRESHOLD, as in bbn_infer.py. Per variant: the members scored and gated, the
shortlist size, how many of these are also on the run's shortlist (kept) and how many not
(new), and the rank of the run's first members.

    python bbn_report.py [--in bbn/bbn_cs3_corpus.json]
"""

from __future__ import annotations

from bbn_infer import corpus_path_argument, load_corpus, rank_corpus
from cs3_config import SENSITIVITY_VARIANTS, SHORTLIST_THRESHOLD

TRACKED = 3          # the run's first members, whose rank is printed per variant


def shortlist(rows, threshold=SHORTLIST_THRESHOLD) -> list[str]:
    """The pids of the gated members below the threshold, in ranking order."""
    return [r["pid"] for r in rows if r["passes_gate"] and r["p_genuine"] < threshold]


def compare(rows, base: list[str], tracked: list[str]) -> dict:
    """A variant's ranking against the run's shortlist `base`."""
    short = shortlist(rows)
    kept = sum(pid in set(base) for pid in short)
    return {"scored": len(rows), "gated": sum(r["passes_gate"] for r in rows),
            "size": len(short), "kept": kept, "new": len(short) - kept,
            "ranks": [short.index(pid) + 1 if pid in short else None for pid in tracked],
            "shortlist": short}


def run_variants(data, variants=SENSITIVITY_VARIANTS) -> tuple[dict, list[dict]]:
    """The run and every variant, each compared with the run's shortlist."""
    base_rows = rank_corpus(data)
    base = shortlist(base_rows)
    tracked = base[:TRACKED]
    run = {"group": "run", "label": "run", **compare(base_rows, base, tracked)}
    results = [{"group": group, "label": label, **compare(rank_corpus(data, **kwargs), base, tracked)}
               for group, entries in variants.items() for label, kwargs in entries]
    return run, results


def always_shortlisted(run: dict, results: list[dict]) -> list[str]:
    """The run's shortlisted members that every variant shortlists too."""
    common = set(run["shortlist"])
    for result in results:
        common &= set(result["shortlist"])
    return [pid for pid in run["shortlist"] if pid in common]


def print_results(run: dict, results: list[dict]) -> None:
    print(f"=== SHORTLIST STABILITY: one setting changed at a time "
          f"(the run: {run['size']} shortlisted of {run['gated']} gated, "
          f"{run['scored']} scored) ===")
    print("  kept: also on the run's shortlist; ranks: of the run's first "
          f"{len(run['ranks'])} on the variant's shortlist (-: not on it)")
    print(f"  {'variant':<30}{'scored':>8}{'gated':>7}{'size':>6}{'kept':>6}{'new':>5}  ranks")
    group = None
    for r in [run] + results:
        if r["group"] != group:
            group = r["group"]
            print()
        ranks = ", ".join("-" if k is None else str(k) for k in r["ranks"])
        print(f"  {r['label']:<30}{r['scored']:>8}{r['gated']:>7}{r['size']:>6}"
              f"{r['kept']:>6}{r['new']:>5}  {ranks}")
    common = always_shortlisted(run, results)
    print(f"\n  on the shortlist under every variant: {len(common)} of {run['size']}")


def main():
    in_path = corpus_path_argument("CS3 sensitivity analysis of the PC member citation BBN.")
    run, results = run_variants(load_corpus(in_path))
    print_results(run, results)


if __name__ == "__main__":
    main()
