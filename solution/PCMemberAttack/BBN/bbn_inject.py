"""
Plants coerced citations in the seats of members that no variant of the model shortlists,
and counts how many of them each variant shortlists then. Writes bbn_cs3_injection.csv
beside the corpus.

A coerced seat: the member reviews k = ceil(REVIEWS_PER_PAPER * papers / pc_size) papers of
the edition and each of those cites `extra` more of the member's works; the reviewed papers
that did not cite the member yet, in proportion, become citing papers.

    python bbn_inject.py [--in bbn/bbn_cs3_corpus.json]
"""

from __future__ import annotations

import csv
import math
import random
import statistics

from bbn_infer import corpus_path_argument, load_corpus, rank_corpus
from cs3_config import (INJECT_EXTRA, INJECT_MEMBERS, INJECT_SEATS, INJECT_SEED,
                        REVIEWS_PER_PAPER, SHORTLIST_THRESHOLD)

VARIANTS = {"depth": {}, "no depth": {"depth_edge": math.inf, "manip_deep": 0.0}}

COLUMNS = ["variant", "extra", "seats", "members", "shortlisted", "shortlisted_ungated",
           "others_shortlisted", "median_injected"]


def review_load(seat, reviews=REVIEWS_PER_PAPER) -> int:
    """reviews * papers / pc_size, rounded up, at most papers."""
    return min(seat["papers"], math.ceil(reviews * seat["papers"] / seat["pc_size"]))


def coerce(seat, extra, reviews=REVIEWS_PER_PAPER) -> dict:
    k = review_load(seat, reviews)
    new_citing = round(k * (1 - seat["citing"] / seat["papers"]))
    return {**seat, "cites": seat["cites"] + k * extra, "citing": seat["citing"] + new_citing,
            "cites_gate": seat["cites_gate"] + k * extra}


def inject(data, order, extra, n_seats, reviews=REVIEWS_PER_PAPER) -> dict:
    """The corpus with the first `n_seats` seats in `order` (pid -> seat indexes) of every
    member in it coerced."""
    members = dict(data["members"])
    for pid, indexes in order.items():
        member = members[pid]
        seats = list(member["seats"])
        for i in indexes[:n_seats]:
            seats[i] = coerce(seats[i], extra, reviews)
        members[pid] = {**member, "seats": seats, "cites_gate_max": max(
            member["cites_gate_max"], *(s["cites_gate"] for s in seats))}
    return {**data, "members": members}


def shortlisted(rows, gated=True, threshold=SHORTLIST_THRESHOLD) -> set[str]:
    return {r["pid"] for r in rows
            if r["p_genuine"] < threshold and (r["passes_gate"] or not gated)}


def draw(rows_of, seats_of, n_members=INJECT_MEMBERS, seed=INJECT_SEED) -> dict:
    """pid -> a random order of their seats, for `n_members` members scored by every variant
    and shortlisted, gated or not, by none."""
    rng = random.Random(seed)
    flagged = set().union(*(shortlisted(rows, gated=False) for rows in rows_of.values()))
    pool = sorted(set.intersection(*({r["pid"] for r in rows} for rows in rows_of.values()))
                  - flagged)
    return {pid: rng.sample(range(len(seats_of[pid])), len(seats_of[pid]))
            for pid in rng.sample(pool, min(n_members, len(pool)))}


def run(data, variants=VARIANTS, extras=INJECT_EXTRA, seat_counts=INJECT_SEATS,
        n_members=INJECT_MEMBERS, seed=INJECT_SEED, reviews=REVIEWS_PER_PAPER):
    """(the shortlist of every variant without injection, one result row per variant, extra
    and number of coerced seats)."""
    base = {name: rank_corpus(data, **kwargs) for name, kwargs in variants.items()}
    order = draw(base, {pid: m["seats"] for pid, m in data["members"].items()}, n_members, seed)
    loads = [review_load(data["members"][pid]["seats"][indexes[0]], reviews)
             for pid, indexes in order.items()]
    results = []
    for extra in extras:
        for n_seats in seat_counts:
            injected = inject(data, order, extra, n_seats, reviews)
            for name, kwargs in variants.items():
                rows = rank_corpus(injected, **kwargs)
                gated, ungated = shortlisted(rows), shortlisted(rows, gated=False)
                results.append({
                    "variant": name, "extra": extra, "seats": n_seats, "members": len(order),
                    "shortlisted": len(gated & order.keys()),
                    "shortlisted_ungated": len(ungated & order.keys()),
                    "others_shortlisted": len(gated - order.keys()),
                    "median_injected": statistics.median(loads) * extra if loads else 0,
                })
    return {name: shortlisted(rows) for name, rows in base.items()}, results


def print_results(base, results) -> None:
    for name, shortlist in base.items():
        print(f"{name}: {len(shortlist)} shortlisted without injection")
    print(f"\n{'variant':<9} {'extra':>5} {'seats':>5} {'injected':>8} "
          f"{'shortlisted':>16} {'ungated':>16} {'others':>6}")
    for r in results:
        n = r["members"]
        print(f"{r['variant']:<9} {r['extra']:>5} {r['seats']:>5} {r['median_injected']:>8g} "
              f"{r['shortlisted']:>5} ({r['shortlisted'] / n:>6.1%}) "
              f"{r['shortlisted_ungated']:>5} ({r['shortlisted_ungated'] / n:>6.1%}) "
              f"{r['others_shortlisted']:>6}")


def main():
    in_path = corpus_path_argument("CS3 injection test of the PC member citation BBN.")
    data = load_corpus(in_path)
    base, results = run(data)
    print(f"{INJECT_MEMBERS} members, seed {INJECT_SEED}, {REVIEWS_PER_PAPER} reviews per "
          f"paper; injected: the median citations planted in a coerced seat")
    print_results(base, results)
    out_path = in_path.resolve().parent / "bbn_cs3_injection.csv"
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(results)
    print(f"\nWrote {out_path}")


if __name__ == "__main__":
    main()
