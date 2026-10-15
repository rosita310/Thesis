"""
Injection test: how many coerced papers does an editor need before the model
shortlists them?

Plants coerced papers in the record of every scored editor, in a journal on
whose board they sit, scores the editor again and counts how often they fall
below SHORTLIST_THRESHOLD, with and without the in/out gate. A planted
co-authorship copies the fit, network band and career stage of a real
co-authorship of the same state in that journal, so it differs from a genuine
one only in its state. Writes bbn_cs2_injection.csv beside the corpus.

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
from cs2_config import (INJECT_COAUTHOR_SWEEP, INJECT_COAUTHORS, INJECT_HOST_BANDS,
                        INJECT_PAPERS, INJECT_PROFILES, INJECT_SEED, MANIP_DIST, PRIOR,
                        SHORTLIST_THRESHOLD)

# The node fields a score depends on; planted nodes that agree on all of them merge.
SCORING_KEYS = ("journal", "state", "career_stage", "fit", "network_band")


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
    if profile == "model":
        states, weights = zip(*MANIP_DIST.items())
        return [(rng.choices(states, weights)[0], 1 / coauthors)
                for _ in range(papers * coauthors)]
    if profile == "follow-up":
        return [("outside_1", 1 / coauthors)] * (papers * coauthors)
    raise ValueError(f"unknown injection profile {profile!r}")


class Donors:
    """The real scored co-authorships a planted one copies its fit and career stage from."""

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


def planted_nodes(journal, profile, papers, rng, donors, repeated_min,
                  coauthors=INJECT_COAUTHORS):
    """The co-authorships of `papers` coerced papers in `journal`."""
    nodes = []
    for state, load in planted_states(profile, papers, rng, repeated_min, coauthors):
        donor = donors.draw(journal, state, rng)
        nodes.append({"journal": journal, "state": state, "load": load,
                      "career_stage": donor.get("career_stage", "established"),
                      "fit": donor.get("fit", ""),
                      "network_band": donor.get("network_band")})
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


def planted_log_odds(planted, own, ref):
    """What the planted nodes add to the editor's log-odds.

    The baseline leaves out only the editor's real nodes: the planted ones were
    never counted in it, so there is nothing of theirs to take out.
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
    """(profile, co-authors per paper): every profile at INJECT_COAUTHORS, and the
    one-off profile again at each count of INJECT_COAUTHOR_SWEEP."""
    return ([(p, INJECT_COAUTHORS) for p in INJECT_PROFILES]
            + [("one-off", c) for c in INJECT_COAUTHOR_SWEEP])


def run(data):
    """One trial per scored editor, run and number of planted papers."""
    ref = reference(data)
    donors = Donors(data["coauthorships"], ref.publisher_of)
    repeated_min = data["config"]["repeated_min"]
    trials = []
    for row in rank_corpus(data, inout_gate=False):
        points = row["inout_points"] or []
        seats = [p for p in points if p["journal"] in ref.journals]
        if not seats:
            continue
        real = row["woe"] * math.log(10)
        band = host_band(row["n_suspect_papers"])
        for profile, coauthors in runs():
            for papers in INJECT_PAPERS:
                rng = random.Random(f"{INJECT_SEED}|{row['ident']}|{profile}|"
                                    f"{coauthors}|{papers}")
                journal = rng.choice(seats)["journal"]
                planted = planted_nodes(journal, profile, papers, rng, donors,
                                        repeated_min, coauthors)
                p = log_odds_to_p(real + planted_log_odds(planted, row["nodes"], ref))
                gate = passes_inout_gate(with_planted_seat(points, journal, len(planted)))
                trials.append({"profile": profile, "coauthors": coauthors,
                               "papers": papers, "band": band, "p": p, "gate": gate})
    return trials


def summarise(trials):
    """Per run, number of papers and host band (and over all bands): the share
    shortlisted without and with the gate, and the median P(genuine)."""
    groups = defaultdict(list)
    for t in trials:
        key = (t["profile"], t["coauthors"], t["papers"])
        groups[key + (t["band"],)].append(t)
        groups[key + ("all",)].append(t)
    rows = []
    for (profile, coauthors, papers, band), ts in groups.items():
        short = [t for t in ts if t["p"] < SHORTLIST_THRESHOLD]
        rows.append({"profile": profile, "coauthors": coauthors, "papers": papers,
                     "host_band": band, "hosts": len(ts),
                     "shortlisted": len(short) / len(ts),
                     "shortlisted_gated": sum(t["gate"] for t in short) / len(ts),
                     "median_p": statistics.median(t["p"] for t in ts)})
    order = {r: i for i, r in enumerate(runs())}
    rows.sort(key=lambda r: (order[(r["profile"], r["coauthors"])], r["papers"],
                             r["host_band"] != "all", r["host_band"]))
    return rows


def write_csv(path, rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        for r in rows:
            writer.writerow({**r, "shortlisted": round(r["shortlisted"], 4),
                             "shortlisted_gated": round(r["shortlisted_gated"], 4),
                             "median_p": round(r["median_p"], 4)})


def print_table(rows):
    bands = sorted({r["host_band"] for r in rows} - {"all"},
                   key=lambda b: int(b.lstrip("<=>").split("-")[0]))
    at = {(r["profile"], r["coauthors"], r["papers"], r["host_band"]): r for r in rows}
    first = runs()[0]
    hosts = {b: at[first + (0, b)]["hosts"] for b in ["all"] + bands}
    print(f"\n=== INJECTION TEST: share shortlisted (P < {SHORTLIST_THRESHOLD}) ===")
    print("  hosts: " + ", ".join(f"{b} own papers {hosts[b]}" for b in bands)
          + f", all {hosts['all']}")
    for profile, coauthors in runs():
        print(f"\n  {profile}, {coauthors} co-authors per coerced paper")
        print(f"  {'papers':>6} {'no gate':>8} {'gate':>8} {'median P':>9}  "
              + "".join(f"{'own ' + b:>11}" for b in bands))
        for papers in INJECT_PAPERS:
            a = at[(profile, coauthors, papers, "all")]
            print(f"  {papers:>6} {a['shortlisted']:>8.1%} {a['shortlisted_gated']:>8.1%} "
                  f"{a['median_p']:>9.3f}  "
                  + "".join(f"{at[(profile, coauthors, papers, b)]['shortlisted']:>11.1%}"
                            for b in bands))


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
