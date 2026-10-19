"""
Injection test: would the model find an editor who built a career on coercion?

Makes fake editors, one at a time, and scores each against the real corpus, without
the in/out gate. A fake editor has a record of INJECT_RECORDS papers in the journal
they edit, a share INJECT_SHARES of them coerced.

- The genuine papers are real co-authorships of other editors in that journal, drawn
  at random until their load makes up the genuine papers (the last one counts only
  with the load still needed). They keep their real ties and co-author habits and,
  like any editor's own co-authorships, are left out of the baselines.
- The coerced papers have INJECT_COAUTHORS co-authors each, who share nothing with the
  editor outside the journal. Each victim's one-off habit is that of a random real
  co-author in the journal. In the one-off profile every coerced paper has new
  victims, in the standing profile the same victims sign every coerced paper, and in
  the both profile half of the coerced papers are one-off and half standing.

Every journal whose load can supply the genuine part and still keep its own baseline
gets INJECT_DRAWS fake editors per setting. A fake editor on two boards has half of
their record in a second such journal of the same publisher. Writes
bbn_cs2_injection.csv beside the corpus.

    python bbn_inject.py [--in bbn/bbn_cs2_corpus.json]
"""

from __future__ import annotations

import bisect
import csv
import math
import random
import statistics
from collections import defaultdict

from bbn_infer import (corpus_path_argument, load_corpus, rank_corpus, reference,
                       score_editor)
from cs2_config import (FALLBACK_MIN_N, INJECT_BOARDS, INJECT_COAUTHORS, INJECT_DRAWS,
                        INJECT_PROFILES, INJECT_RECORDS, INJECT_SEED, INJECT_SHARES,
                        SHORTLIST_THRESHOLD)


# ---------------------------------------------------------------------------
# The fake editor
# ---------------------------------------------------------------------------

def coerced_count(record, share):
    """The coerced papers of a record, rounded half up: half of 15 is 8."""
    return int(record * share + 0.5)


def split(n, parts):
    """`n` divided over `parts` as evenly as possible, the larger part first."""
    return [n // parts + (i < n % parts) for i in range(parts)]


def coerced_states(profile, papers, repeated_min, coauthors=INJECT_COAUTHORS):
    """(state, load) of the co-authorships of `papers` coerced papers; the loads add
    up to `papers`."""
    if papers == 0:
        return []
    if profile == "one-off":
        return [("single", 1 / coauthors)] * (papers * coauthors)
    if profile == "standing":
        state = "repeated" if papers >= repeated_min else "single"
        return [(state, papers / coauthors)] * coauthors
    if profile == "both":
        one_off, standing = split(papers, 2)
        return (coerced_states("one-off", one_off, repeated_min, coauthors)
                + coerced_states("standing", standing, repeated_min, coauthors))
    raise ValueError(f"unknown injection profile {profile!r}")


class Journals:
    """The real scored co-authorships per journal: the genuine papers and the victims'
    habits of a fake editor are drawn from them."""

    def __init__(self, coauthorships, publisher_of):
        self.publisher_of = publisher_of
        self.nodes = defaultdict(list)
        for node in coauthorships.values():
            self.nodes[node["journal"]].append(node)

    def load(self, journal):
        return sum(n["load"] for n in self.nodes.get(journal, []))

    def eligible(self, records=INJECT_RECORDS, min_n=FALLBACK_MIN_N):
        """The journals that can supply the largest genuine part and still keep at least
        N_min of load for their own baseline, sorted."""
        return sorted(j for j in self.nodes if self.load(j) >= max(records) + min_n)

    def genuine(self, journal, papers, rng):
        """(nodes, own) for `papers` genuine papers: real co-authorships drawn without
        replacement until their load makes up `papers`. `nodes` are what the fake
        editor is scored on, the last one cut to the load still needed; `own` are the
        originals, which the baselines leave out."""
        nodes, own, needed = [], [], float(papers)
        for node in rng.sample(self.nodes[journal], len(self.nodes[journal])):
            if needed <= 1e-9:
                break
            nodes.append({**node, "load": min(node["load"], needed)})
            own.append(node)
            needed -= node["load"]
        return nodes, own

    def victim(self, journal, rng):
        """A random real co-authorship of the journal, whose co-author lends a coerced
        co-author their habit."""
        return rng.choice(self.nodes[journal])


def coerced_nodes(journal, profile, papers, rng, journals, repeated_min):
    """The co-authorships of `papers` coerced papers in `journal`."""
    nodes = []
    for state, load in coerced_states(profile, papers, repeated_min):
        donor = journals.victim(journal, rng)
        nodes.append({"journal": journal, "state": state, "load": load,
                      "career_stage": donor["career_stage"],
                      "pool_n": donor["pool_n"], "pool_oneoffs": donor["pool_oneoffs"]})
    return nodes


def fake_editor(seats, record, share, profile, rng, journals, repeated_min):
    """(nodes, own) of a fake editor whose record is spread evenly over `seats`."""
    nodes, own = [], []
    coerced = coerced_count(record, share)
    for journal, n_coerced, n_genuine in zip(seats, split(coerced, len(seats)),
                                             split(record - coerced, len(seats))):
        genuine, originals = journals.genuine(journal, n_genuine, rng)
        nodes += genuine + coerced_nodes(journal, profile, n_coerced, rng, journals,
                                         repeated_min)
        own += originals
    return nodes, own


# ---------------------------------------------------------------------------
# The test
# ---------------------------------------------------------------------------

def settings():
    """(profile, boards, record, share) of every kind of fake editor: every profile on
    one board, and the one-off profile on each number of boards in INJECT_BOARDS."""
    kinds = [(p, 1) for p in INJECT_PROFILES] + [("one-off", b) for b in INJECT_BOARDS
                                                 if b > 1]
    return [(p, b, r, s) for p, b in kinds for r in INJECT_RECORDS for s in INJECT_SHARES]


def seats_for(journal, boards, eligible, publisher_of, rng):
    """`journal` and `boards - 1` other eligible journals of its publisher, or None."""
    others = [j for j in eligible if j != journal
              and publisher_of.get(j) == publisher_of.get(journal)]
    if len(others) < boards - 1:
        return None
    return [journal] + rng.sample(others, boards - 1)


def rank_among(woe, real_woes):
    """The rank a weight of evidence takes among the real editors' (sorted), 1 = lowest."""
    return bisect.bisect_left(real_woes, woe) + 1


def run(data):
    """One trial per fake editor."""
    ref = reference(data)
    journals = Journals(data["coauthorships"], ref.publisher_of)
    eligible = journals.eligible()
    repeated_min = data["config"]["repeated_min"]
    real_woes = sorted(r["woe"] for r in rank_corpus(data, inout_gate=False))
    trials = []
    for profile, boards, record, share in settings():
        for journal in eligible:
            for draw in range(INJECT_DRAWS):
                rng = random.Random(f"{INJECT_SEED}|{profile}|{boards}|{record}|"
                                    f"{share:.4f}|{journal}|{draw}")
                seats = seats_for(journal, boards, eligible, ref.publisher_of, rng)
                if seats is None:
                    continue
                nodes, own = fake_editor(seats, record, share, profile, rng, journals,
                                         repeated_min)
                p, log_odds, _ = score_editor(nodes, ref, own=own)
                woe = log_odds / math.log(10)
                trials.append({"profile": profile, "boards": boards, "record": record,
                               "share": share, "journal": journal, "p": p, "woe": woe,
                               "rank": rank_among(woe, real_woes)})
    return trials, len(real_woes), eligible


def summarise(trials):
    """Per kind of fake editor: how many there are, the share found (P below the
    threshold), and the median P and rank."""
    groups = defaultdict(list)
    for t in trials:
        groups[(t["profile"], t["boards"], t["record"], t["share"])].append(t)
    rows = []
    for (profile, boards, record, share), ts in groups.items():
        rows.append({"profile": profile, "boards": boards, "record": record,
                     "share": round(share, 4), "coerced": coerced_count(record, share),
                     "fakes": len(ts),
                     "found": sum(t["p"] < SHORTLIST_THRESHOLD for t in ts) / len(ts),
                     "median_p": statistics.median(t["p"] for t in ts),
                     "median_rank": statistics.median(t["rank"] for t in ts),
                     "worst_rank": max(t["rank"] for t in ts)})
    order = {s[:3] + (round(s[3], 4),): i for i, s in enumerate(settings())}
    rows.sort(key=lambda r: order[(r["profile"], r["boards"], r["record"], r["share"])])
    return rows


def write_csv(path, rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        for r in rows:
            writer.writerow({**r, "found": round(r["found"], 4),
                             "median_p": round(r["median_p"], 6)})


def print_table(rows, n_real, eligible):
    print(f"\n=== INJECTION TEST: fake editors scored against {n_real} real editors, "
          f"no gate ===")
    print(f"  {len(eligible)} journals, {INJECT_DRAWS} fake editors per journal and kind; "
          f"found = P < {SHORTLIST_THRESHOLD}; rank 1 = most suspect")
    print(f"  {'profile':<9}{'boards':>7}{'record':>7}{'coerced':>8}{'fakes':>7}"
          f"{'found':>8}{'median P':>11}{'median rank':>12}{'worst rank':>11}")
    for r in rows:
        print(f"  {r['profile']:<9}{r['boards']:>7}{r['record']:>7}{r['coerced']:>8}"
              f"{r['fakes']:>7}{r['found']:>8.1%}{r['median_p']:>11.2e}"
              f"{r['median_rank']:>12.0f}{r['worst_rank']:>11}")


def main():
    in_path = corpus_path_argument("CS2 injection test: fake editors who built a career "
                                   "on coercion.")
    data = load_corpus(in_path)
    trials, n_real, eligible = run(data)
    rows = summarise(trials)
    out_path = in_path.resolve().parent / "bbn_cs2_injection.csv"
    write_csv(out_path, rows)
    print_table(rows, n_real, eligible)
    print(f"\nWrote {out_path}")


if __name__ == "__main__":
    main()
