"""
Scores every editor in the corpus from bbn_extract.py on P(genuine | evidence)
and writes, beside the corpus, bbn_cs2_ranking.csv (one row per ranked editor)
and bbn_cs2_scored.json (the same editors with the per-node evidence).

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
                        FIT_STATES, INOUT_GATE, MANIP_DIST, MANIP_FIT, PRIOR,
                        PSEUDO_COUNT, SHORTLIST_THRESHOLD)

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

    journals      journal -> {"publisher", "baseline_counts": {career_stage: {state: load}}}
    pooled        publisher -> career_stage -> {state: load}, summed over its journals
    states        the corpus states, in the corpus's order
    fit_base      publisher -> state -> network band -> {fit: load}; None without fit
    publisher_of  journal -> publisher
    """
    journals: dict
    pooled: dict
    states: list
    fit_base: dict | None
    publisher_of: dict


def reference(data) -> Reference:
    states = data["config"]["states"]
    journals = data["journals"]
    publisher_of = {j: info.get("publisher", "") for j, info in journals.items()}
    pooled = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    for journal, info in journals.items():
        for stage, counts in info["baseline_counts"].items():
            for s in states:
                pooled[publisher_of[journal]][stage][s] += counts.get(s, 0.0)
    return Reference(journals, pooled, states, data.get("fit_baseline"), publisher_of)


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


def subtract_own(counts, own, keys, field, match):
    """counts minus the load of the editor's own matching nodes, binned by `field`.

    `field` is "state" for the state baseline and "fit" for the fit baseline; a
    node with no value there (no fit measured) contributes nothing either way.
    """
    out = {k: counts.get(k, 0.0) for k in keys}
    for node in own:
        value = node.get(field)
        if value and match(node):
            out[value] = out.get(value, 0.0) - node["load"]
    return out


def genuine_dist(ref: Reference, journal, stage, own,
                 condition_on_career_stage=CONDITION_ON_CAREER_STAGE):
    """g(s | journal) = P(state | genuine, journal), without the `own` nodes.

    Returns (smoothed_dist, fallback_level). A level is used when its load after
    removing `own` reaches FALLBACK_MIN_N; the pooled level, over the journals of
    the same publisher, is used regardless.
    """
    states = ref.states
    by_stage = ref.journals.get(journal, {}).get("baseline_counts", {})
    publisher = ref.publisher_of.get(journal, "")
    pooled = ref.pooled.get(publisher, {})

    def same_publisher(n):
        return ref.publisher_of.get(n["journal"], "") == publisher

    def level(counts, match, label):
        counts = subtract_own(counts, own, states, "state", match)
        return (smooth(counts, states), label) if sum(counts.values()) >= FALLBACK_MIN_N \
            else None

    if condition_on_career_stage and by_stage.get(stage):
        hit = level(by_stage[stage], lambda n: n["journal"] == journal
                    and n["career_stage"] == stage, "journal:career_stage")
        if hit:
            return hit

    hit = level(merge(by_stage.values(), states),
                lambda n: n["journal"] == journal, "journal")
    if hit:
        return hit

    if condition_on_career_stage:
        hit = level(pooled.get(stage, {}),
                    lambda n: same_publisher(n) and n["career_stage"] == stage,
                    "pooled:career_stage")
        if hit:
            return hit

    everything = subtract_own(merge(pooled.values(), states), own, states,
                              "state", same_publisher)
    return smooth(everything, states), "pooled"


def fit_dist(ref: Reference, journal, state, band, own):
    """g(f | s, beta) = P(fit | genuine, state, network band), without the `own`
    nodes, within the journal's publisher.

    Falls back from state and band, to state, to uniform, as genuine_dist does.
    """
    publisher = ref.publisher_of.get(journal, "")
    bands = (ref.fit_base or {}).get(publisher, {}).get(state, {})
    key = str(band)

    def same_publisher(n):
        return ref.publisher_of.get(n["journal"], "") == publisher

    def level(counts, match, label):
        counts = subtract_own(counts, own, FIT_STATES, "fit", match)
        return (smooth(counts, FIT_STATES), label) \
            if sum(counts.values()) >= FALLBACK_MIN_N else None

    hit = level(bands.get(key, {}),
                lambda n: same_publisher(n) and n["state"] == state
                and str(n.get("network_band")) == key, "state:band")
    if hit:
        return hit

    hit = level(merge(bands.values(), FIT_STATES),
                lambda n: same_publisher(n) and n["state"] == state, "state")
    if hit:
        return hit

    return {f: 1.0 / len(FIT_STATES) for f in FIT_STATES}, "uniform"


# ---------------------------------------------------------------------------
# Inference  (genuine-space: LR < 1 lowers genuineness)
# ---------------------------------------------------------------------------

def log_odds_to_p(log_odds):
    """P(genuine) from log-odds, overflow-safe."""
    if log_odds >= 0:
        return 1.0 / (1.0 + math.exp(-log_odds))
    e = math.exp(log_odds)
    return e / (1.0 + e)


def node_lr(state, gdist, alpha, manip=MANIP_DIST, fit="", fdist=None,
            manip_fit=MANIP_FIT):
    """LR for one co-authorship, over the pair (state, fit); on the state alone without a fit."""
    g = gdist[state]
    m = manip[state]
    if fit and fdist:
        g *= fdist[fit]
        m *= manip_fit[state][fit]
    return g / (alpha * m + (1 - alpha) * g)


def score_editor(nodes, ref: Reference, alpha=ALPHA, prior=PRIOR,
                 manip=MANIP_DIST, condition_on_career_stage=CONDITION_ON_CAREER_STAGE,
                 manip_fit=MANIP_FIT):
    """log_odds = log prior_odds + SUM load * log LR over one editor's nodes.

    Returns (P(genuine), log_odds, detail), with one detail record per node.
    """
    log_odds = math.log(prior / (1 - prior))
    detail = []
    for node in nodes:
        gdist, level = genuine_dist(ref, node["journal"], node["career_stage"], nodes,
                                    condition_on_career_stage=condition_on_career_stage)
        fit = node.get("fit", "") if ref.fit_base else ""
        fdist, flevel = (fit_dist(ref, node["journal"], node["state"],
                                  node.get("network_band"), nodes)
                         if fit else (None, ""))
        lr = node_lr(node["state"], gdist, alpha, manip, fit, fdist, manip_fit)
        contribution = node["load"] * math.log(lr)
        log_odds += contribution
        detail.append({
            "coauthor": node.get("coauthor_name", node.get("coauthor_id")),
            "journal": node["journal"], "state": node["state"],
            "career_stage": node["career_stage"],
            "coauthor_pubs": node.get("coauthor_pubs"),
            "load": round(node["load"], 4),
            "n_shared_suspect": node.get("n_shared_suspect"),
            "n_shared_inside": node.get("n_shared_inside"),
            "n_shared_outside": node.get("n_shared_outside"),
            "fit": fit, "fit_pct": node.get("fit_pct"),
            "network_band": node.get("network_band"),
            "fit_fallback_level": flevel,
            "fit_genuine_p": round(fdist[fit], 6) if fit and fdist else "",
            "fallback_level": level, "genuine_p": round(gdist[node["state"]], 6),
            "lr": round(lr, 6), "weighted_log_lr": round(contribution, 6),
        })
    return log_odds_to_p(log_odds), log_odds, detail


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
    """At least one board seat with x >= y (in-journal vs. outside co-authors)."""
    return bool(points) and any(p["x"] >= p["y"] for p in points)


def reddest_point(points):
    """The board seat with the largest x - y."""
    return max(points, key=lambda p: p["x"] - p["y"]) if points else None


# ---------------------------------------------------------------------------
# Ranking
# ---------------------------------------------------------------------------

def rank_corpus(data, alpha=ALPHA, prior=PRIOR, manip=MANIP_DIST,
                condition_on_career_stage=CONDITION_ON_CAREER_STAGE,
                inout_gate=INOUT_GATE) -> list[dict]:
    """One row per ranked editor, lowest weight of evidence (`woe`) first.

    Every editor is scored; `inout_gate` only decides who is ranked.
    """
    ref = reference(data)
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
            "inout_journal": (reddest or {}).get("journal", ""),
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
                   "inout_journal", "inout_x", "inout_y",
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
        print(f"     {d['state']:<9} load={d['load']:>5.2f} "
              f"{d['coauthor'][:26]:<26} pubs={str(d['coauthor_pubs']):>5} "
              f"[{d['fallback_level']:<14}] genuine_p={d['genuine_p']:.4f} "
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
    print(f"Corpus: repeated_min={data['config']['repeated_min']}, "
          f"junior_max_pubs={data['config']['junior_max_pubs']}")
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
                   "shortlist_threshold": SHORTLIST_THRESHOLD,
                   "repeated_min": data["config"]["repeated_min"],
                   "n_ranked": len(records),
                   "n_shortlisted": sum(1 for e in records if e["shortlisted"]),
                   "editors": records},
                  f, ensure_ascii=False, indent=2)
    print(f"Wrote {json_path}")

    print_shortlist(shortlist, records)


if __name__ == "__main__":
    main()
