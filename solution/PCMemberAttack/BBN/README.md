# PC member citation BBN

Scores every PC member of an LNCS conference on P(genuine) with a Bayesian belief network over
their seats. All settings are in `cs3_config.py`; the scripts take only paths on the command line.


## Input

All from the database, with the connection from `solution/.env`:

- `lncs_front_matter.committee_member`: the parsed committees, one row per person and role per
  edition, with the matched dblp pid.
- `dblp_dump.lncs_volume`, `lncs_article` and `lncs_article_author`: the LNCS volumes, their
  papers and authors. An edition is a volume key without its `-N` suffix.
- `opencitations_dump.reference`: the references of the LNCS papers.
- `dblp_dump.cited_publication` and `cited_publication_author`: the cited works and their authors.

## Citations

A reference resolves to a dblp work through any of its DOIs (`cited_dois`) and counts once for
every author of that work. A self-citation is one where the cited author also wrote the citing
paper. References to dblp records without authors drop out.

## Seats

A seat is a member with a pid in the role `pc` or `senior_pc` of one edition. Per seat:

| Column         | Meaning                                                                    |
|----------------|----------------------------------------------------------------------------|
| `x`            | references to the member from the edition, the member's own papers left out |
| `x_westerbaan` | references to the member from the edition, self-citations included         |
| `n`            | papers of the edition with a reference resolved to dblp, the member's own left out |
| `k_off`, `n_off` | the same `x` and `n`, summed over the member's off-power editions, or over the peers |
| `src`          | where the rate comes from: `conference` or `peers`                         |
| `span`         | with `conference`: years between the seat and the furthest edition taken   |
| `rate`         | `(k_off + RATE_PSEUDO_COUNT) / n_off`                                      |
| `k_peers`, `n_peers`, `rate_peers` | the same, over the other seats of the edition          |
| `E`            | `max(rate, rate_peers) * n`, the citations expected                        |
| `surplus`      | `x - E`                                                                    |

The corpus adds `pubs`: the member's dblp publications of a type in `PUB_TYPES` from before the
edition's year.

An edition is off-power for a member when they hold no role of any kind there, also not under an
unmatched name they carry elsewhere, and its committee is plausibly complete (`MIN_PC_SIZE`,
`MIN_PC_SHARE`). Only main editions are off-power editions, never a satellite. Years are the
conference's, from the edition key.

The off-power editions of a seat are those of its stream within `RATE_WINDOW` years, and, while
they hold fewer than `MIN_OFF_PAPERS` papers, further out nearest first, up to `MAX_SPAN` years. A
satellite seat takes the rate of its main conference. Otherwise the rate is that of the other
seats of the edition, if there are at least `MIN_PC_SIZE - 1` of them; else the seat has no rate.

## The model

The corpus keeps the seats with a rate and `n > 0`. A member is scored with at least `MIN_SEATS`
seats, and ranked when one of all their seats has more than `GATE_REFERENCES` references
(`x_westerbaan`).

The evidence of a seat is the bin of its surplus (`STATES`, lower edges `SURPLUS_EDGES`). Its
genuine baseline `g(b | src, band)` has two observed parents: `src`, and a band, of `E`
(`PROM_BANDS`) with `src = conference`, of `pubs` (`PUBS_BANDS`) with `src = peers`. `g` is the
share of seats in each bin with the same `src` and band, in the seat's conference (stream,
satellites included), else its CORE rank, else all conferences: the first level that keeps
`FALLBACK_MIN_N` seats after leaving out the scored member's, the last regardless. Every bin gets
`PSEUDO_COUNT`. All seats in the corpus count in the baseline.

A seat in bin `b` adds `log LR`, with `LR = g(b) / (ALPHA m(b) + (1 - ALPHA) g(b))` and `m(b)` =
`MANIP_DIST`, to the prior log-odds of `PRIOR`. `W` is the sum in log10. A member is shortlisted
below `SHORTLIST_THRESHOLD`.

## Pipeline

`bbn_extract.py` builds the corpus in stages:

| Stage          | Reads                      | Builds                                                           |
|----------------|----------------------------|------------------------------------------------------------------|
| `citations`    | database                   | `lncs_citation.citation` and `lncs_citation.paper` (rebuilt)     |
| `seats`        | database                   | `bbn/seats.csv`, with the funnel and the gate count              |
| `publications` | dump, `bbn/seats.csv`      | `bbn/publications.csv`: publications per member and year         |
| `corpus`       | the two CSVs above         | `bbn/bbn_cs3_corpus.json`: the seats per member, with the funnel |

`bbn_infer.py` scores the corpus and writes `bbn/bbn_cs3_ranking.csv` (the gated members),
`bbn/bbn_cs3_ranking_ungated.csv` (every scored member) and `bbn/bbn_cs3_scored.json` (with the
evidence per seat). It prints the gated shortlist and the rank of the members in `WATCH_PIDS`.

```
python bbn_extract.py --stage citations
python bbn_extract.py --stage seats
python bbn_extract.py --stage publications   # one pass over the dump, or --dump
python bbn_extract.py --stage corpus
python bbn_infer.py
```

Only `citations` and `seats` need the database; the rest runs on the standard library.
`test_bbn_extract.py` and `test_bbn_infer.py` run without data.
