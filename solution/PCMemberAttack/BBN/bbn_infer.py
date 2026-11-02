"""
Scores every PC member in the corpus from bbn_extract.py on P(genuine | evidence)
and writes, beside the corpus, bbn_cs3_ranking.csv (the members who pass the
gate), bbn_cs3_ranking_ungated.csv (every scored member) and bbn_cs3_scored.json
(every scored member with the per-seat evidence).

Every seat adds log LR of its surplus bin to the member's log-odds. The network
is G -> bin <- src, band (both observed).

    python bbn_infer.py [--in bbn/bbn_cs3_corpus.json]
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from bisect import bisect_right
from collections import Counter, defaultdict
from pathlib import Path

from cs3_config import (ALPHA, BBN_DIR, FALLBACK_MIN_N, GATE_REFERENCES, MANIP_DIST,
                        MIN_SEATS, PRIOR, PROM_BANDS, PSEUDO_COUNT, PUBS_BANDS,
                        SHORTLIST_THRESHOLD, STATES, SURPLUS_EDGES, WATCH_PIDS)

DEFAULT_IN = BBN_DIR / "bbn_cs3_corpus.json"

# Console layout only.
SHORTLIST_PRINT_CAP = 40    # the CSV always holds every member
BREAKDOWN_PRINT_CAP = 12


def load_corpus(in_path):
    """Read a corpus JSON and check that the configured model fits its states."""
    in_path = Path(in_path)
    if not in_path.exists():
        raise SystemExit(f"Run bbn_extract.py --stage corpus first; {in_path} not found.")
    if set(MANIP_DIST) != set(STATES) or len(STATES) != len(SURPLUS_EDGES) + 1:
        raise SystemExit(f"MANIP_DIST keys and SURPLUS_EDGES must match STATES {STATES}")
    return json.loads(in_path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# The evidence of a seat: the bin of its surplus, and its band
# ---------------------------------------------------------------------------

def surplus_bin(surplus, edges=SURPLUS_EDGES, states=STATES) -> str:
    return states[bisect_right(edges, surplus)]


def band_label(value, bands) -> str:
    """'<0.5', '0.5-2', '2-5', '>=5' for the bands [0.5, 2, 5]."""
    i = bisect_right(bands, value)
    if i == 0:
        return f"<{bands[0]:g}"
    if i == len(bands):
        return f">={bands[-1]:g}"
    return f"{bands[i - 1]:g}-{bands[i]:g}"


def seat_band(seat, bands=PROM_BANDS, pubs_bands=PUBS_BANDS) -> str:
    """prom, the band of E; with the peer rate, the band of the member's publications."""
    if seat["src"] == "peers":
        return "pubs " + band_label(seat["pubs"], pubs_bands)
    return "E " + band_label(seat["E"], bands)


def classify(seat, edges=SURPLUS_EDGES, bands=PROM_BANDS, pubs_bands=PUBS_BANDS) -> dict:
    surplus = seat["x"] - seat["E"]
    return {**seat, "surplus": surplus, "state": surplus_bin(surplus, edges),
            "band": seat_band(seat, bands, pubs_bands)}


# ---------------------------------------------------------------------------
# Genuine column g: leave-one-member-out baseline, smoothing and fallback
# ---------------------------------------------------------------------------

def cells(seat) -> list[tuple]:
    """The baseline cells of a seat, narrowest first: its conference, its CORE rank,
    all conferences; each with the seat's rate source and band."""
    return [(level, key, seat["src"], seat["band"]) for level, key in
            (("conference", seat["stream"]), ("core_rank", seat["core_rank"]),
             ("pooled", ""))]


def bin_counts(seats) -> dict:
    """cell -> {state: seats}."""
    counts = defaultdict(Counter)
    for seat in seats:
        for cell in cells(seat):
            counts[cell][seat["state"]] += 1
    return counts


def smooth(counts, states=STATES):
    """Every bin gets the pseudo-count lambda (PSEUDO_COUNT), then normalise."""
    total = sum(counts.get(s, 0) for s in states) + PSEUDO_COUNT * len(states)
    return {s: (counts.get(s, 0) + PSEUDO_COUNT) / total for s in states}


def genuine_dist(counts, seat, own_counts, min_n=FALLBACK_MIN_N, states=STATES):
    """g(b | src, band) at the narrowest level that keeps `min_n` seats once the
    member's own (`own_counts`) are left out; the pooled level regardless.
    Returns (smoothed_dist, level)."""
    for cell in cells(seat):
        left = {s: counts[cell].get(s, 0) - own_counts.get(cell, {}).get(s, 0)
                for s in states}
        if sum(left.values()) >= min_n or cell[0] == "pooled":
            return smooth(left, states), cell[0]


# ---------------------------------------------------------------------------
# Inference  (genuine-space: LR < 1 lowers genuineness)
# ---------------------------------------------------------------------------

def log_odds_to_p(log_odds):
    """P(genuine) from log-odds, overflow-safe."""
    if log_odds >= 0:
        return 1.0 / (1.0 + math.exp(-log_odds))
    e = math.exp(log_odds)
    return e / (1.0 + e)


def seat_lr(state, gdist, alpha, manip=MANIP_DIST):
    """LR = g(b) / (alpha m(b) + (1 - alpha) g(b)) for one seat in bin b."""
    g = gdist[state]
    return g / (alpha * manip[state] + (1 - alpha) * g)


def score_member(seats, counts, alpha=ALPHA, prior=PRIOR, manip=MANIP_DIST,
                 min_n=FALLBACK_MIN_N):
    """log_odds = log prior_odds + SUM log LR over one member's seats.
    Returns (P(genuine), log_odds, detail), with one detail record per seat."""
    own_counts = bin_counts(seats)
    log_odds = math.log(prior / (1 - prior))
    detail = []
    for seat in seats:
        gdist, level = genuine_dist(counts, seat, own_counts, min_n)
        lr = seat_lr(seat["state"], gdist, alpha, manip)
        log_odds += math.log(lr)
        detail.append(seat_detail(seat, gdist, level, lr))
    return log_odds_to_p(log_odds), log_odds, detail


def seat_detail(seat, gdist, level, lr) -> dict:
    return {
        "edition_key": seat["edition_key"], "role": seat["role"],
        "x": seat["x"], "x_westerbaan": seat["x_westerbaan"], "n": seat["n"],
        "E": seat["E"], "pubs": seat.get("pubs"), "surplus": round(seat["surplus"], 4),
        "state": seat["state"], "src": seat["src"], "band": seat["band"],
        "fallback_level": level,
        "genuine_p": round(gdist[seat["state"]], 6),
        "lr": round(lr, 6), "woe": round(math.log10(lr), 6),
    }


def passes_gate(member, gate=GATE_REFERENCES) -> bool:
    """Westerbaan's gate: a seat with more than `gate` references to the member."""
    return member["x_westerbaan_max"] > gate


# ---------------------------------------------------------------------------
# Ranking
# ---------------------------------------------------------------------------

def rank_corpus(data, alpha=ALPHA, prior=PRIOR, manip=MANIP_DIST, edges=SURPLUS_EDGES,
                bands=PROM_BANDS, pubs_bands=PUBS_BANDS, min_seats=MIN_SEATS,
                min_n=FALLBACK_MIN_N, gate=GATE_REFERENCES) -> list[dict]:
    """One row per member with at least `min_seats` seats, lowest weight of evidence
    (`woe`) first. Every seat in the corpus counts in the baseline."""
    seats_of = {pid: [classify(s, edges, bands, pubs_bands) for s in m["seats"]]
                for pid, m in data["members"].items()}
    counts = bin_counts(s for seats in seats_of.values() for s in seats)

    rows = []
    for pid, seats in seats_of.items():
        if len(seats) < min_seats:
            continue
        member = data["members"][pid]
        post, log_odds, detail = score_member(seats, counts, alpha, prior, manip, min_n)
        states = Counter(s["state"] for s in seats)
        rows.append({
            "name": member["name"], "pid": pid,
            "p_genuine": post, "woe": log_odds / math.log(10),
            "n_seats": len(seats),
            **{f"n_{s}": states.get(s, 0) for s in STATES},
            "max_surplus": max(s["surplus"] for s in seats),
            "x_westerbaan_max": member["x_westerbaan_max"],
            "passes_gate": passes_gate(member, gate),
            "streams": "|".join(sorted({s["stream"] for s in seats})),
            "detail": detail,
        })
    rows.sort(key=lambda r: r["woe"])
    return rows


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

RANKING_COLUMNS = (["rank", "name", "pid", "p_genuine", "woe", "n_seats"]
                   + [f"n_{s}" for s in STATES]
                   + ["max_surplus", "x_westerbaan_max", "passes_gate", "streams"])


def write_ranking_csv(path, rows) -> None:
    """One summary row per ranked member, most suspicious first."""
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=RANKING_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for i, r in enumerate(rows, 1):
            writer.writerow({**r, "rank": i, "p_genuine": f"{r['p_genuine']:.6g}",
                             "woe": round(r["woe"], 3), "max_surplus": round(r["max_surplus"], 3)})


def member_records(rows, threshold=SHORTLIST_THRESHOLD) -> list[dict]:
    """Every scored member with their per-seat evidence, most incriminating first."""
    return [{
        "name": r["name"], "pid": r["pid"], "p_genuine": r["p_genuine"],
        "woe": round(r["woe"], 3), "shortlisted": r["p_genuine"] < threshold,
        "passes_gate": r["passes_gate"], "n_seats": r["n_seats"],
        "x_westerbaan_max": r["x_westerbaan_max"], "streams": r["streams"],
        "seats": sorted(r["detail"], key=lambda d: d["woe"]),
    } for r in rows]


def fmt_p(p):
    """A non-zero posterior never shows as 0.0000."""
    return f"{p:.4f}" if p >= 1e-4 else f"{p:.2e}"


def print_shortlist(shortlist) -> None:
    print("\n=== INVESTIGATION PRIORITY (gated, lowest genuineness first) ===")
    print("  (WoE = log10 posterior-odds of genuine; more negative = stronger evidence"
          " against.)")
    for r in shortlist[:SHORTLIST_PRINT_CAP]:
        print(f"  P(genuine)={fmt_p(r['p_genuine']):>9}  WoE={r['woe']:>7.2f}  "
              f"{r['name'][:28]:<28} seats={r['n_seats']:<3} "
              + " ".join(f"{s}={r[f'n_{s}']:<2}" for s in STATES[1:])
              + f" max surplus={r['max_surplus']:>6.1f}")
    if len(shortlist) > SHORTLIST_PRINT_CAP:
        print(f"  ... (+{len(shortlist) - SHORTLIST_PRINT_CAP} more shortlisted; see the CSV)")
    if shortlist:
        print_breakdown(shortlist[0], "lowest-scoring member")


def print_breakdown(row, label) -> None:
    print(f"\n=== EVIDENCE BREAKDOWN ({label}: {row['name']!r}) ===")
    print("  (LR<1 lowers genuineness)")
    seats = sorted(row["detail"], key=lambda d: d["woe"])
    for d in seats[:BREAKDOWN_PRINT_CAP]:
        print(f"     {d['edition_key'][:24]:<24} {d['state']:<8} x={d['x']:<4} "
              f"E={d['E']:>7.2f} surplus={d['surplus']:>7.2f} "
              f"[{d['src']:<10} {d['band']:<12} {d['fallback_level']:<10}] "
              f"g={d['genuine_p']:.4f} LR={d['lr']:>6.3f} woe={d['woe']:>+7.3f}")
    if len(seats) > BREAKDOWN_PRINT_CAP:
        print(f"     ... (+{len(seats) - BREAKDOWN_PRINT_CAP} more seats)")


def print_watched(rows, gated, data) -> None:
    rank_all = {r["pid"]: i for i, r in enumerate(rows, 1)}
    rank_gated = {r["pid"]: i for i, r in enumerate(gated, 1)}
    by_pid = {r["pid"]: r for r in rows}
    for pid in WATCH_PIDS:
        r = by_pid.get(pid)
        if r is None:
            n = len(data["members"].get(pid, {}).get("seats", []))
            print(f"\n{pid}: not scored ({n} seats in the corpus)")
            continue
        where = (f"gated rank {rank_gated[pid]} of {len(gated)}" if pid in rank_gated
                 else "not gated")
        print(f"\n{r['name']}: P(genuine)={fmt_p(r['p_genuine'])}, WoE={r['woe']:.2f}, "
              f"{where}, ungated rank {rank_all[pid]} of {len(rows)}")
        print_breakdown(r, "watched")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def corpus_path_argument(description) -> Path:
    """The --in option: the corpus JSON."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--in", dest="in_path", default=DEFAULT_IN,
                        help="corpus JSON from bbn_extract.py --stage corpus")
    return Path(parser.parse_args().in_path)


def main():
    in_path = corpus_path_argument("CS3 PC member citation BBN inference.")
    data = load_corpus(in_path)
    rows = rank_corpus(data)
    gated = [r for r in rows if r["passes_gate"]]
    shortlist = [r for r in gated if r["p_genuine"] < SHORTLIST_THRESHOLD]
    n_short_all = sum(1 for r in rows if r["p_genuine"] < SHORTLIST_THRESHOLD)

    print(f"Model: {data['model']} | alpha={ALPHA} | prior P(genuine)={PRIOR} | "
          f"MANIP_DIST={MANIP_DIST}")
    print(f"Surplus edges {SURPLUS_EDGES}, prom bands {PROM_BANDS}, publication bands "
          f"{PUBS_BANDS} (peer rate), at least {MIN_SEATS} seats, N_min={FALLBACK_MIN_N}")
    print(f"Ungated: {len(rows)} members; {n_short_all} below threshold {SHORTLIST_THRESHOLD}.")
    print(f"Gated (a seat with more than {GATE_REFERENCES} references): {len(gated)} members; "
          f"{len(shortlist)} below threshold {SHORTLIST_THRESHOLD}.")

    out_dir = in_path.resolve().parent
    for name, ranked in (("bbn_cs3_ranking.csv", gated),
                         ("bbn_cs3_ranking_ungated.csv", rows)):
        write_ranking_csv(out_dir / name, ranked)
        print(f"Wrote {out_dir / name}")

    records = member_records(rows)
    json_path = out_dir / "bbn_cs3_scored.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump({"alpha": ALPHA, "prior": PRIOR, "manip_dist": MANIP_DIST,
                   "surplus_edges": SURPLUS_EDGES, "prom_bands": PROM_BANDS,
                   "pubs_bands": PUBS_BANDS,
                   "min_seats": MIN_SEATS, "fallback_min_n": FALLBACK_MIN_N,
                   "gate_references": GATE_REFERENCES,
                   "shortlist_threshold": SHORTLIST_THRESHOLD,
                   "n_scored": len(records), "n_gated": len(gated),
                   "members": records},
                  f, ensure_ascii=False, indent=1)
    print(f"Wrote {json_path}")

    print_shortlist(shortlist)
    print_watched(rows, gated, data)


if __name__ == "__main__":
    main()
