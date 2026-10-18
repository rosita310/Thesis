"""
Injection test: how many coerced papers does an editor need before the model
shortlists them?

Plants coerced papers in the record of one scored editor at a time, in a journal on
whose board they sit, scores that editor again and counts how often they fall
below SHORTLIST_THRESHOLD, with and without the in/out gate. The other editors and
the baselines stay as they are. A planted co-authorship copies the career stage and
the pool stats (the co-author's own collaborators and one-off collaborators) of a
real co-authorship of the same state in that journal, so it gets the personal
adjustment a real co-author of that tie gets, and differs from a genuine one only in
its state.

Every profile is planted in one seat. An editor on two or more boards also gets the
one-off profile spread over two of their seats. The results are split by the
editor's own record (INJECT_HOST_BANDS) and for the editors on two or more boards.
Writes bbn_cs2_injection.csv beside the corpus.

    python bbn_inject.py [--in bbn/bbn_cs2_corpus.json]
"""

from __future__ import annotations

import csv
import math
import random
import statistics
from collections import defaultdict

from bbn_infer import (corpus_path_argument, load_corpus, log_odds_to_p,
                       passes_inout_gate, rank_corpus, reference, score_editor)
from cs2_config import (INJECT_COAUTHORS, INJECT_HOST_BANDS, INJECT_PAPERS,
                        INJECT_PROFILES, INJECT_SEED, MANIP_DIST, PRIOR,
                        SHORTLIST_THRESHOLD)

# The node fields a score depends on; planted nodes that agree on all of them merge.
SCORING_KEYS = ("journal", "state", "career_stage", "pool_n", "pool_oneoffs")

MULTI_BOARD = "multi-board"   # the host group of editors with two or more seats


# ---------------------------------------------------------------------------
# Planting
# ---------------------------------------------------------------------------

def planted_states(profile, papers, rng, repeated_min, coauthors=INJECT_COAUTHORS):
    """(state, load) of every planted co-authorship; the loads add up to `papers`."""
    if papers == 0:
        return []
    if profile == "one-off":
        return [("single", 1 / coauthors)] * (papers * coauthors)
    if profile == "standing":
        state = "repeated" if papers >= repeated_min else "single"
        return [(state, papers / coauthors)] * coauthors
    if profile == "mix":
        states, weights = zip(*MANIP_DIST.items())
        return [(rng.choices(states, weights)[0], 1 / coauthors)
                for _ in range(papers * coauthors)]
    if profile == "follow-up":
        return [("outside_1", 1 / coauthors)] * (papers * coauthors)
    raise ValueError(f"unknown injection profile {profile!r}")


class Donors:
    """The real scored co-authorships a planted one copies its career stage and pool
    stats from."""

    def __init__(self, coauthorships, publisher_of):
        self.publisher_of = publisher_of
        self.by_journal = defaultdict(list)
        self.by_publisher = defaultdict(list)
        for node in coauthorships.values():
            self.by_journal[(node["journal"], node["state"])].append(node)
            self.by_publisher[(publisher_of.get(node["journal"], ""), node["state"])].append(node)

    def draw(self, journal, state, rng):
        """A real co-authorship of `state` in `journal`, else in its publisher, else none."""
        pool = (self.by_journal.get((journal, state))
                or self.by_publisher.get((self.publisher_of.get(journal, ""), state)))
        return rng.choice(pool) if pool else {}


def planted_nodes(journal, profile, papers, rng, donors, repeated_min):
    """The co-authorships of `papers` coerced papers in `journal`."""
    nodes = []
    for state, load in planted_states(profile, papers, rng, repeated_min):
        donor = donors.draw(journal, state, rng)
        nodes.append({"journal": journal, "state": state, "load": load,
                      "career_stage": donor.get("career_stage", "established"),
                      "pool_n": donor.get("pool_n"),
                      "pool_oneoffs": donor.get("pool_oneoffs")})
    return nodes


def merged(nodes):
    """Planted nodes with the same scoring fields summed into one; a node adds
    load * log LR, so the score is unchanged."""
    out = {}
    for node in nodes:
        key = tuple(node[k] for k in SCORING_KEYS)
        if key in out:
            out[key]["load"] += node["load"]
        else:
            out[key] = dict(node)
    return list(out.values())


def with_planted_seat(points, journal, added):
    """The editor's in/out points after `added` new co-authors in `journal`."""
    return [{**p, "x": p["x"] + added} if p["journal"] == journal else p
            for p in points]


def split(papers, n_seats):
    """`papers` divided over `n_seats` seats as evenly as possible, the larger part first."""
    return [papers // n_seats + (i < papers % n_seats) for i in range(n_seats)]


def planted_log_odds(planted, own, ref):
    """What the planted nodes add to the editor's log-odds.

    The baselines (the journal column and r_bar) leave out only the editor's real
    nodes: the planted ones were never counted in them, so there is nothing of
    theirs to take out.
    """
    _, log_odds, _ = score_editor(merged(planted), ref, own=own)
    return log_odds - math.log(PRIOR / (1 - PRIOR))


# ---------------------------------------------------------------------------
# The test
# ---------------------------------------------------------------------------

def host_band(n_papers, edges=INJECT_HOST_BANDS):
    """The band of the editor's own suspect papers: <=2 | 3-10 | >10."""
    low = 0
    for edge in edges:
        if n_papers <= edge:
            return f"{low + 1}-{edge}" if low else f"<={edge}"
        low = edge
    return f">{edges[-1]}"


def runs():
    """(profile, seats the coerced papers are spread over): every profile in one
    seat, and the one-off profile over two seats of an editor on two or more boards."""
    return [(p, 1) for p in INJECT_PROFILES] + [("one-off", 2)]


def plant(row, profile, n_seats, papers, seats, ref, donors, repeated_min):
    """P(genuine) and the gate of one editor after planting `papers` coerced papers
    spread over `n_seats` of their `seats`."""
    rng = random.Random(f"{INJECT_SEED}|{row['ident']}|{profile}|{n_seats}|{papers}")
    points = row["inout_points"]
    planted = []
    for journal, k in zip(rng.sample(seats, n_seats), split(papers, n_seats)):
        nodes = planted_nodes(journal, profile, k, rng, donors, repeated_min)
        planted += nodes
        points = with_planted_seat(points, journal, len(nodes))
    real = row["woe"] * math.log(10)
    p = log_odds_to_p(real + planted_log_odds(planted, row["nodes"], ref))
    return p, passes_inout_gate(points)


def run(data):
    """One trial per scored editor, run and number of planted papers."""
    ref = reference(data)
    donors = Donors(data["coauthorships"], ref.publisher_of)
    repeated_min = data["config"]["repeated_min"]
    trials = []
    for row in rank_corpus(data, inout_gate=False):
        seats = [p["journal"] for p in row["inout_points"] or []
                 if p["journal"] in ref.journals]
        if not seats:
            continue
        host = {"band": host_band(row["n_suspect_papers"]), "multi_board": len(seats) >= 2}
        for profile, n_seats in runs():
            if n_seats > len(seats):
                continue
            for papers in INJECT_PAPERS:
                p, gate = plant(row, profile, n_seats, papers, seats, ref, donors,
                                repeated_min)
                trials.append({"profile": profile, "seats": n_seats, "papers": papers,
                               **host, "p": p, "gate": gate})
    return trials


def host_groups(trial):
    """The host groups a trial counts in: all, its record band, and multi-board."""
    return ["all", trial["band"]] + ([MULTI_BOARD] if trial["multi_board"] else [])


def summarise(trials):
    """Per run, number of papers and host group: the share shortlisted without and
    with the gate, and the median P(genuine)."""
    groups = defaultdict(list)
    for t in trials:
        for group in host_groups(t):
            groups[(t["profile"], t["seats"], t["papers"], group)].append(t)
    rows = []
    for (profile, seats, papers, group), ts in groups.items():
        short = [t for t in ts if t["p"] < SHORTLIST_THRESHOLD]
        rows.append({"profile": profile, "seats": seats, "papers": papers,
                     "hosts": group, "n_hosts": len(ts),
                     "shortlisted": len(short) / len(ts),
                     "shortlisted_gated": sum(t["gate"] for t in short) / len(ts),
                     "median_p": statistics.median(t["p"] for t in ts)})
    order = {r: i for i, r in enumerate(runs())}
    group_order = ["all"] + host_band_names() + [MULTI_BOARD]
    rows.sort(key=lambda r: (order[(r["profile"], r["seats"])], r["papers"],
                             group_order.index(r["hosts"])))
    return rows


def host_band_names(edges=INJECT_HOST_BANDS):
    """The record bands in order: <=2, 3-10, >10."""
    return [host_band(edge) for edge in edges] + [host_band(edges[-1] + 1)]


def write_csv(path, rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        for r in rows:
            writer.writerow({**r, "shortlisted": round(r["shortlisted"], 4),
                             "shortlisted_gated": round(r["shortlisted_gated"], 4),
                             "median_p": round(r["median_p"], 4)})


def print_table(rows):
    at = {(r["profile"], r["seats"], r["papers"], r["hosts"]): r for r in rows}
    groups = ["all"] + host_band_names() + [MULTI_BOARD]
    print(f"\n=== INJECTION TEST: share shortlisted (P < {SHORTLIST_THRESHOLD}), "
          f"with the gate / without ===")
    print("  hosts: " + ", ".join(f"{g} {at[('one-off', 1, 0, g)]['n_hosts']}"
                                  for g in groups))
    for profile, seats in runs():
        where = "in one seat" if seats == 1 else f"spread over {seats} seats"
        print(f"\n  {profile}, {INJECT_COAUTHORS} co-authors per coerced paper, {where}")
        print(f"  {'papers':>6}" + "".join(f"{g:>16}" for g in groups))
        for papers in INJECT_PAPERS:
            cells = []
            for g in groups:
                r = at.get((profile, seats, papers, g))
                cells.append(f"{r['shortlisted_gated']:>7.1%} /{r['shortlisted']:>6.1%}"
                             if r else f"{'-':>16}")
            print(f"  {papers:>6}" + "".join(f"{c:>16}" for c in cells))


def main():
    in_path = corpus_path_argument("CS2 injection test: detection of planted coerced papers.")
    data = load_corpus(in_path)
    rows = summarise(run(data))
    out_path = in_path.resolve().parent / "bbn_cs2_injection.csv"
    write_csv(out_path, rows)
    print_table(rows)
    print(f"\nWrote {out_path}")


if __name__ == "__main__":
    main()
