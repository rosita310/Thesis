"""
Scores every editor in the corpus from bbn_extract.py on P(genuine | evidence)
and writes, beside the corpus, bbn_cs2_ranking.csv (one row per ranked editor)
and bbn_cs2_scored.json (the same editors with the per-node evidence).

Every co-authorship adds load * log LR of its tie to the editor's log-odds. The
genuine column of a tie is the journal's, without the editor's own nodes.
g(single) is scaled by the co-author's one-off habit over the journal's
typical habit, and the journal's column is that of its recent co-authorships for a
recent one and of its earlier ones for the rest. The network is
G -> tie <- the co-author's one-off habit and the period (both observed).

    python bbn_infer.py [--in bbn/bbn_cs2_corpus.json]
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from cs2_config import (ALPHA, BBN_DIR, CONDITION_ON_CAREER_STAGE, FALLBACK_MIN_N,
                        INOUT_GATE, MANIP_DIST, PRIOR, PSEUDO_COUNT,
                        RECENT_FROM, SHORTLIST_THRESHOLD)

DEFAULT_IN = BBN_DIR / "bbn_cs2_corpus.json"

# Console layout only.
SHORTLIST_PRINT_CAP = 40    # the CSV always holds every editor
BREAKDOWN_PRINT_CAP = 12


# ---------------------------------------------------------------------------
# The corpus and its reference distributions
# ---------------------------------------------------------------------------

def load_corpus(in_path):
    """Read a corpus JSON and check that its states match the configured model."""
    in_path = Path(in_path)
    if not in_path.exists():
        raise SystemExit(f"Run bbn_extract.py --stage corpus first; {in_path} not found.")
    data = json.loads(in_path.read_text(encoding="utf-8"))
    if set(MANIP_DIST) != set(data["config"]["states"]):
        raise SystemExit("MANIP_DIST keys must match the corpus states: "
                         f"{sorted(MANIP_DIST)} vs {sorted(data['config']['states'])}")
    return data


@dataclass(frozen=True)
class Reference:
    """The genuine reference distributions of one corpus, as the scoring needs them.

    journals        journal -> {"publisher", "baseline_counts": {career_stage: {state: load}}}
    pooled          publisher -> career_stage -> {state: load}, summed over its journals
    states          the corpus states, in the corpus's order
    publisher_of    journal -> publisher
    pool_min        collaborators a co-author needs for the personal adjustment;
                    math.inf switches the adjustment off
    oneoff_journal  journal -> (sum of load * habit, sum of load) over the scored
                    co-authorships that reach pool_min
    oneoff_pooled   publisher -> the same, summed over its journals
    recent_from     the first year of a recent co-authorship; None: no split
    recent_journal  journal -> {state: load} of its recent co-authorships
    recent_pooled   publisher -> the same, summed over its journals
    """
    journals: dict
    pooled: dict
    states: list
    publisher_of: dict
    pool_min: float
    oneoff_journal: dict
    oneoff_pooled: dict
    recent_from: int | None = None
    recent_journal: dict | None = None
    recent_pooled: dict | None = None


def period(node, recent_from):
    """"recent" if the co-authorship's first shared publication is from `recent_from`
    on, else "earlier"; None when `recent_from` is None."""
    if recent_from is None:
        return None
    return "recent" if (node.get("first_year") or 0) >= recent_from else "earlier"


def reference(data, pool_min=FALLBACK_MIN_N, recent_from=RECENT_FROM) -> Reference:
    states = data["config"]["states"]
    journals = data["journals"]
    publisher_of = {j: info.get("publisher", "") for j, info in journals.items()}
    pooled = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    for journal, info in journals.items():
        for stage, counts in info["baseline_counts"].items():
            for s in states:
                pooled[publisher_of[journal]][stage][s] += counts.get(s, 0.0)
    nodes = data.get("coauthorships", {}).values()
    by_journal, by_publisher = oneoff_sums(nodes, publisher_of, pool_min)
    recent_journal = defaultdict(lambda: defaultdict(float))
    recent_pooled = defaultdict(lambda: defaultdict(float))
    for node in nodes:
        if period(node, recent_from) == "recent":
            recent_journal[node["journal"]][node["state"]] += node["load"]
            recent_pooled[publisher_of.get(node["journal"], "")][node["state"]] += node["load"]
    return Reference(journals, pooled, states, publisher_of, pool_min,
                     by_journal, by_publisher, recent_from, recent_journal, recent_pooled)


# ---------------------------------------------------------------------------
# Genuine column g: leave-one-editor-out baseline, smoothing and fallback
# ---------------------------------------------------------------------------

def smooth(counts, states):
    """Every bin gets the pseudo-count lambda (PSEUDO_COUNT), then normalise."""
    total = sum(counts.get(s, 0.0) for s in states) + PSEUDO_COUNT * len(states)
    return {s: (counts.get(s, 0.0) + PSEUDO_COUNT) / total for s in states}


def merge(count_dicts, keys):
    """Sum several {key: load} dicts into one, over `keys`."""
    total = {k: 0.0 for k in keys}
    for counts in count_dicts:
        for k in keys:
            total[k] += counts.get(k, 0.0)
    return total


def subtract_own(counts, own, states, match):
    """{state: load} minus the load of the editor's own matching nodes."""
    out = {s: counts.get(s, 0.0) for s in states}
    for node in own:
        if match(node):
            out[node["state"]] = out.get(node["state"], 0.0) - node["load"]
    return out


def in_period(counts, recent, when, states):
    """The part of `counts` in period `when`: the recent counts, or the rest."""
    if when == "recent":
        return {s: recent.get(s, 0.0) for s in states}
    return {s: counts.get(s, 0.0) - recent.get(s, 0.0) for s in states}


def genuine_dist(ref: Reference, journal, stage, own,
                 condition_on_career_stage=CONDITION_ON_CAREER_STAGE, when=None):
    """g(s | journal) = P(state | genuine, journal), without the `own` nodes.

    Returns (smoothed_dist, fallback_level). A level is used when its load after
    removing `own` reaches FALLBACK_MIN_N; the pooled level, over the journals of
    the same publisher, is used regardless. With a period `when` ("recent" or
    "earlier"), the journal and the pool are first tried on that period alone.
    """
    states = ref.states
    by_stage = ref.journals.get(journal, {}).get("baseline_counts", {})
    publisher = ref.publisher_of.get(journal, "")
    pooled = ref.pooled.get(publisher, {})

    def same_publisher(n):
        return ref.publisher_of.get(n["journal"], "") == publisher

    def level(counts, match, label):
        counts = subtract_own(counts, own, states, match)
        return (smooth(counts, states), label) if sum(counts.values()) >= FALLBACK_MIN_N \
            else None

    if condition_on_career_stage and by_stage.get(stage):
        hit = level(by_stage[stage], lambda n: n["journal"] == journal
                    and n["career_stage"] == stage, "journal:career_stage")
        if hit:
            return hit

    in_journal = merge(by_stage.values(), states)
    if when:
        hit = level(in_period(in_journal, ref.recent_journal.get(journal, {}), when, states),
                    lambda n: n["journal"] == journal
                    and period(n, ref.recent_from) == when, f"journal:{when}")
        if hit:
            return hit

    hit = level(in_journal, lambda n: n["journal"] == journal, "journal")
    if hit:
        return hit

    if condition_on_career_stage:
        hit = level(pooled.get(stage, {}),
                    lambda n: same_publisher(n) and n["career_stage"] == stage,
                    "pooled:career_stage")
        if hit:
            return hit

    in_pool = merge(pooled.values(), states)
    if when:
        hit = level(in_period(in_pool, ref.recent_pooled.get(publisher, {}), when, states),
                    lambda n: same_publisher(n) and period(n, ref.recent_from) == when,
                    f"pooled:{when}")
        if hit:
            return hit

    everything = subtract_own(in_pool, own, states, same_publisher)
    return smooth(everything, states), "pooled"


# ---------------------------------------------------------------------------
# The personal one-off baseline: g(single) scaled by the co-author's own habit
# ---------------------------------------------------------------------------

def oneoff_habit(node):
    """The co-author's one-off habit (k + lambda) / (n + 2 lambda): how often they
    publish only once with a collaborator, from pool_oneoffs (k) of pool_n (n)."""
    return ((node["pool_oneoffs"] + PSEUDO_COUNT)
            / (node["pool_n"] + 2 * PSEUDO_COUNT))


def qualifies(node, pool_min) -> bool:
    """True if the co-author has the pool_min collaborators the adjustment needs."""
    return node.get("pool_n") is not None and node["pool_n"] >= pool_min


def oneoff_sums(nodes, publisher_of, pool_min) -> tuple[dict, dict]:
    """(sum of load * habit, sum of load) over the qualifying nodes, per journal
    and per publisher."""
    by_journal = defaultdict(lambda: [0.0, 0.0])
    by_publisher = defaultdict(lambda: [0.0, 0.0])
    for node in nodes:
        if not qualifies(node, pool_min):
            continue
        weighted = node["load"] * oneoff_habit(node)
        for sums in (by_journal[node["journal"]],
                     by_publisher[publisher_of.get(node["journal"], "")]):
            sums[0] += weighted
            sums[1] += node["load"]
    return dict(by_journal), dict(by_publisher)


def without_own(sums, own, match, pool_min) -> tuple[float, float]:
    """(weighted, load) minus the editor's own qualifying nodes that `match`."""
    weighted, load = sums
    for node in own:
        if match(node) and qualifies(node, pool_min):
            weighted -= node["load"] * oneoff_habit(node)
            load -= node["load"]
    return weighted, load


def typical_habit(ref: Reference, journal, own):
    """The journal's typical one-off habit, and where it came from.

    This is the load-weighted mean one-off habit over the journal's co-authors with
    enough collaborators, leaving out the scored editor's own co-authorships
    (`own`). If less than FALLBACK_MIN_N of load is left, all journals of the same
    publisher are pooled instead.

    Returns (typical_habit, "journal"), (typical_habit, "pooled"), or (None, "")
    when even the pool has no load left.
    """
    publisher = ref.publisher_of.get(journal, "")
    weighted, load = without_own(ref.oneoff_journal.get(journal, (0.0, 0.0)), own,
                                 lambda n: n["journal"] == journal, ref.pool_min)
    if load >= FALLBACK_MIN_N:
        return weighted / load, "journal"
    weighted, load = without_own(
        ref.oneoff_pooled.get(publisher, (0.0, 0.0)), own,
        lambda n: ref.publisher_of.get(n["journal"], "") == publisher, ref.pool_min)
    if load > 1e-9:
        return weighted / load, "pooled"
    return None, ""


def odds_scaled(p, factor):
    """The probability whose odds are `factor` times the odds of `p`."""
    odds = p / (1 - p) * factor
    return odds / (1 + odds)


def personal_column(gdist, habit_ratio) -> dict:
    """g_i: the journal's column with the odds of `single` multiplied by the habit
    ratio and every other state rescaled by one common factor, so that it sums to 1."""
    single = odds_scaled(gdist["single"], habit_ratio)
    rest = (1 - single) / (1 - gdist["single"])
    return {s: single if s == "single" else p * rest for s, p in gdist.items()}


def personal_dist(ref: Reference, node, gdist, own):
    """The genuine column of one co-authorship: (g_i, the habit ratio, the level of
    the typical habit).

    A co-author below ref.pool_min keeps the journal's column, with no habit ratio.
    """
    if not qualifies(node, ref.pool_min):
        return gdist, None, ""
    typical, level = typical_habit(ref, node["journal"], own)
    if typical is None:
        return gdist, None, ""
    habit_ratio = oneoff_habit(node) / typical
    return personal_column(gdist, habit_ratio), habit_ratio, level


# ---------------------------------------------------------------------------
# Inference  (genuine-space: LR < 1 lowers genuineness)
# ---------------------------------------------------------------------------

def log_odds_to_p(log_odds):
    """P(genuine) from log-odds, overflow-safe."""
    if log_odds >= 0:
        return 1.0 / (1.0 + math.exp(-log_odds))
    e = math.exp(log_odds)
    return e / (1.0 + e)


def node_lr(state, gdist, alpha, manip=MANIP_DIST):
    """LR = g(t) / (alpha m(t) + (1 - alpha) g(t)) for one co-authorship of tie t."""
    g = gdist[state]
    return g / (alpha * manip[state] + (1 - alpha) * g)


def score_editor(nodes, ref: Reference, alpha=ALPHA, prior=PRIOR,
                 manip=MANIP_DIST, condition_on_career_stage=CONDITION_ON_CAREER_STAGE,
                 own=None):
    """log_odds = log prior_odds + SUM load * log LR over one editor's nodes.

    `own` are the editor's nodes that the reference counts, and so the ones the
    baselines (the journal column and the typical habit) leave out; by default all of `nodes`.
    Returns (P(genuine), log_odds, detail), with one detail record per node.
    """
    own = nodes if own is None else own
    log_odds = math.log(prior / (1 - prior))
    detail = []
    for node in nodes:
        gdist, level = genuine_dist(ref, node["journal"], node["career_stage"], own,
                                    condition_on_career_stage=condition_on_career_stage,
                                    when=period(node, ref.recent_from))
        pdist, habit_ratio, oneoff_level = personal_dist(ref, node, gdist, own)
        lr = node_lr(node["state"], pdist, alpha, manip)
        contribution = node["load"] * math.log(lr)
        log_odds += contribution
        detail.append(node_detail(node, gdist, level, pdist, habit_ratio, oneoff_level,
                                  lr, contribution))
    return log_odds_to_p(log_odds), log_odds, detail


def node_detail(node, gdist, level, pdist, habit_ratio, oneoff_level, lr,
                contribution) -> dict:
    """The record of one scored co-authorship: its tie, the journal's and its own
    genuine column, and what it adds to the editor's log-odds."""
    state = node["state"]
    return {
        "coauthor": node.get("coauthor_name", node.get("coauthor_id")),
        "journal": node["journal"], "state": state,
        "career_stage": node["career_stage"],
        "first_year": node.get("first_year"),
        "coauthor_pubs": node.get("coauthor_pubs"),
        "load": round(node["load"], 4),
        "n_shared_suspect": node.get("n_shared_suspect"),
        "n_shared_inside": node.get("n_shared_inside"),
        "n_shared_outside": node.get("n_shared_outside"),
        "fallback_level": level,
        "journal_genuine_p": round(gdist[state], 6),
        "journal_single_p": round(gdist["single"], 6),
        "pool_n": node.get("pool_n"), "pool_oneoffs": node.get("pool_oneoffs"),
        "adjusted": habit_ratio is not None,
        "habit_ratio": round(habit_ratio, 6) if habit_ratio is not None else None,
        "oneoff_level": oneoff_level,
        "single_p": round(pdist["single"], 6),
        "genuine_p": round(pdist[state], 6),
        "lr": round(lr, 6), "weighted_log_lr": round(contribution, 6),
    }


def state_shares(nodes):
    """Load-weighted share of each state over a set of nodes."""
    total = sum(n["load"] for n in nodes)
    shares = defaultdict(float)
    for node in nodes:
        shares[node["state"]] += node["load"]
    return {s: (v / total if total else 0.0) for s, v in shares.items()}, total


# ---------------------------------------------------------------------------
# The entry gate (Westerbaan's in/out boundary)
# ---------------------------------------------------------------------------

def passes_inout_gate(points) -> bool:
    """At least one year on a board with x >= y (co-authors in the journals the editor
    edits that year vs. elsewhere), and x at least 1."""
    return bool(points) and any(p["x"] >= max(p["y"], 1) for p in points)


def reddest_point(points):
    """The year on a board with the largest x - y."""
    return max(points, key=lambda p: p["x"] - p["y"]) if points else None


# ---------------------------------------------------------------------------
# Ranking
# ---------------------------------------------------------------------------

def rank_corpus(data, alpha=ALPHA, prior=PRIOR, manip=MANIP_DIST,
                condition_on_career_stage=CONDITION_ON_CAREER_STAGE,
                inout_gate=INOUT_GATE, pool_min=FALLBACK_MIN_N,
                recent_from=RECENT_FROM) -> list[dict]:
    """One row per ranked editor, lowest weight of evidence (`woe`) first.

    Every editor is scored; `inout_gate` only decides who is ranked. `pool_min` is
    the boundary of the personal adjustment (math.inf: none), `recent_from` the first
    year of a recent co-authorship (None: no split).
    """
    ref = reference(data, pool_min, recent_from)
    all_nodes = data["coauthorships"]
    editors = data.get("editors", {})
    labels = data.get("editor_labels", {})
    controls = data.get("control_coauthorships", {})
    control_index = data.get("control_index", {})

    rows = []
    for ed_id, node_ids in data["editor_index"].items():
        nodes = [all_nodes[n] for n in node_ids if n in all_nodes]
        if not nodes:
            continue
        post, log_odds, detail = score_editor(
            nodes, ref, alpha, prior, manip=manip,
            condition_on_career_stage=condition_on_career_stage)
        info = editors.get(ed_id, {})
        shares, load = state_shares(nodes)
        control_nodes = [controls[n] for n in control_index.get(ed_id, []) if n in controls]
        control_shares, _ = state_shares(control_nodes)
        reddest = reddest_point(info.get("inout_points"))
        rows.append({
            "ident": ed_id,
            "name": labels.get(ed_id, info.get("name", ed_id)),
            "pid": ed_id if info.get("is_pid") else "",
            "publishers": "|".join(sorted({ref.publisher_of.get(n["journal"], "")
                                           for n in nodes} - {""})),
            "p_genuine": post, "woe": log_odds / math.log(10),
            "n_nodes": len(nodes), "load": load,
            "n_suspect_papers": info.get("n_suspect_papers", 0),
            "n_pubs_outside_journal": info.get("n_pubs_outside_journal", 0),
            "inout_points": info.get("inout_points"),
            "inout_year": (reddest or {}).get("year", ""),
            "inout_x": (reddest or {}).get("x", ""),
            "inout_y": (reddest or {}).get("y", ""),
            "passes_inout_gate": passes_inout_gate(info.get("inout_points")),
            "journals": "|".join(info.get("journals", sorted({n["journal"] for n in nodes}))),
            "share_single": shares.get("single", 0.0),
            "share_outside": sum(v for k, v in shares.items()
                                 if k.startswith("outside")),
            "share_outside_1": shares.get("outside_1", 0.0),
            "n_control_nodes": len(control_nodes),
            "control_share_single": control_shares.get("single", 0.0),
            "nodes": nodes,
            "detail": detail,
        })

    if inout_gate:
        if rows and not any(r["inout_points"] for r in rows):
            raise SystemExit("INOUT_GATE is set but the corpus has no in/out points: "
                             "run `python bbn_extract.py --stage editor_authors`, "
                             "then rebuild the corpus with --stage corpus.")
        rows = [r for r in rows if r["passes_inout_gate"]]
    rows.sort(key=lambda r: r["woe"])
    return rows


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

RANKING_COLUMNS = ["name", "pid", "publishers", "p_genuine", "woe", "n_suspect_papers",
                   "n_nodes", "load", "share_single", "share_outside",
                   "share_outside_1", "n_pubs_outside_journal",
                   "inout_year", "inout_x", "inout_y",
                   "passes_inout_gate", "n_control_nodes",
                   "control_share_single", "journals"]


def write_ranking_csv(path, rows) -> None:
    """One summary row per ranked editor, most suspicious first."""
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=RANKING_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            out = dict(r)
            out["p_genuine"] = f"{r['p_genuine']:.6g}"
            for key in ("woe", "load"):
                out[key] = round(r[key], 3)
            for key in ("share_single", "share_outside", "share_outside_1",
                        "control_share_single"):
                out[key] = round(r[key], 4)
            writer.writerow(out)


def editor_records(rows, threshold=SHORTLIST_THRESHOLD) -> list[dict]:
    """Every ranked editor with their per-node evidence, most incriminating first."""
    return [{
        "name": r["name"], "pid": r["pid"], "publishers": r["publishers"],
        "p_genuine": r["p_genuine"],
        "woe": round(r["woe"], 3),
        "shortlisted": r["p_genuine"] < threshold,
        "n_suspect_papers": r["n_suspect_papers"], "n_nodes": r["n_nodes"],
        "load": round(r["load"], 3),
        "journals": r["journals"],
        "n_control_nodes": r["n_control_nodes"],
        "control_share_single": round(r["control_share_single"], 4),
        "coauthorships": sorted(r["detail"], key=lambda d: d["weighted_log_lr"]),
    } for r in rows]


def fmt_p(p):
    """Never render a non-zero posterior as a misleading 0.000."""
    return f"{p:.4f}" if p >= 1e-4 else f"{p:.2e}"


def print_shortlist(shortlist, records) -> None:
    print("\n=== INVESTIGATION PRIORITY (lowest genuineness first) ===")
    print("  (WoE = log10 posterior-odds of genuine; more negative = stronger evidence"
          " against. Load = suspect papers' worth of evidence, so it is the plate size.)")
    for r in shortlist[:SHORTLIST_PRINT_CAP]:
        print(f"  P(genuine)={fmt_p(r['p_genuine']):>9}  WoE={r['woe']:>7.2f}  "
              f"{r['name'][:28]:<28} papers={r['n_suspect_papers']:<3} "
              f"nodes={r['n_nodes']:<3} load={r['load']:>5.1f} "
              f"single={r['share_single']:>5.0%}")
    if len(shortlist) > SHORTLIST_PRINT_CAP:
        print(f"  ... (+{len(shortlist) - SHORTLIST_PRINT_CAP} more shortlisted; "
              f"see the CSV)")
    if not shortlist:
        return

    top = records[0]
    print(f"\n=== EVIDENCE BREAKDOWN (lowest-scoring editor: {top['name']!r}) ===")
    print("  (LR<1 lowers genuineness; the contribution is load * log LR)")
    for d in top["coauthorships"][:BREAKDOWN_PRINT_CAP]:
        habit_ratio = f"{d['habit_ratio']:.2f}" if d["adjusted"] else "-"
        print(f"     {d['state']:<9} load={d['load']:>5.2f} "
              f"{d['coauthor'][:26]:<26} pubs={str(d['coauthor_pubs']):>5} "
              f"[{d['fallback_level']:<14}] journal g={d['journal_genuine_p']:.4f} "
              f"habit ratio={habit_ratio:>5} g={d['genuine_p']:.4f} "
              f"LR={d['lr']:>6.3f} w.logLR={d['weighted_log_lr']:>+7.3f}")
    if len(top["coauthorships"]) > BREAKDOWN_PRINT_CAP:
        print(f"     ... (+{len(top['coauthorships']) - BREAKDOWN_PRINT_CAP} "
              f"more co-authorships)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def corpus_path_argument(description) -> Path:
    """The one command-line option every reader of the corpus shares: which corpus."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--in", dest="in_path", default=DEFAULT_IN,
                        help="corpus JSON from bbn_extract.py --stage corpus")
    return Path(parser.parse_args().in_path)


def main():
    in_path = corpus_path_argument("CS2 co-authorship BBN inference.")
    data = load_corpus(in_path)
    rows = rank_corpus(data)
    shortlist = [r for r in rows if r["p_genuine"] < SHORTLIST_THRESHOLD]

    print(f"Model: {data['model']} | alpha={ALPHA} | prior P(genuine)={PRIOR} | "
          f"MANIP_DIST={MANIP_DIST}")
    print(f"Personal one-off baseline: co-authors with >= {FALLBACK_MIN_N:g} collaborators, "
          "odds of g(single) times the habit ratio")
    print(f"Corpus: repeated_min={data['config']['repeated_min']}, "
          f"junior_max_pubs={data['config']['junior_max_pubs']}, "
          f"coauthor_min_pubs={data['config'].get('coauthor_min_pubs')}")
    print(f"Recent co-authorships (first shared publication from {RECENT_FROM} on) "
          "judged against recent ones" if RECENT_FROM is not None
          else "No split into recent and earlier co-authorships")
    print(f"Ranked {len(rows)} editors"
          + (" (gate: in >= out)" if INOUT_GATE else "")
          + f"; {len(shortlist)} below threshold {SHORTLIST_THRESHOLD}.")

    out_dir = in_path.resolve().parent
    rank_path = out_dir / "bbn_cs2_ranking.csv"
    write_ranking_csv(rank_path, rows)
    print(f"Wrote {rank_path}")

    records = editor_records(rows)
    json_path = out_dir / "bbn_cs2_scored.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump({"alpha": ALPHA, "prior": PRIOR,
                   "manip_dist": MANIP_DIST, "inout_gate": INOUT_GATE,
                   "pool_min": FALLBACK_MIN_N,
                   "shortlist_threshold": SHORTLIST_THRESHOLD,
                   "repeated_min": data["config"]["repeated_min"],
                   "coauthor_min_pubs": data["config"].get("coauthor_min_pubs"),
                   "recent_from": RECENT_FROM,
                   "n_ranked": len(records),
                   "n_shortlisted": sum(1 for e in records if e["shortlisted"]),
                   "editors": records},
                  f, ensure_ascii=False, indent=2)
    print(f"Wrote {json_path}")

    print_shortlist(shortlist, records)


if __name__ == "__main__":
    main()
