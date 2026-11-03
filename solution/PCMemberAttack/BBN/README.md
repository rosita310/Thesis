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

And from `RAW_DATA/opencitations/index` (see `.env`) the OpenCitations Index dump, for the citations
of every cited work.

## Citations

A reference resolves to a dblp work through any of its DOIs (`cited_dois`) and counts once for
every author of that work. A self-citation is one where the cited author also wrote the citing
paper. References to dblp records without authors drop out.

A work is well known at an edition when its citations in the OpenCitations Index up to and
including the edition's year, citations by its own authors left out, are in the top
`WELL_KNOWN_TOP` of the works the stream cites that have a citation by then. A stream with fewer
of those than `WELL_KNOWN_MIN_WORKS` takes the works of all streams.

## Seats

A seat is a member with a pid in the role `pc` or `senior_pc` of one edition. Per seat:

| Column             | Meaning                                                                  |
|--------------------|--------------------------------------------------------------------------|
| `cites`            | references to the member from the edition, the member's own papers and well-known works left out |
| `citing`           | the papers of the edition that `cites` comes from                        |
| `cites_gate`       | references to the member from the edition, all included                  |
| `papers`           | papers of the edition with a reference resolved to dblp, the member's own left out |
| `off_cites`, `off_papers` | the same `cites` and `papers`, summed over the member's off-power editions, or over the peers |
| `src`              | where the rate comes from: `conference` or `peers`                       |
| `span`             | with `conference`: years between the seat and the furthest edition taken |
| `rate`             | `(off_cites + RATE_PSEUDO_COUNT) / off_papers`                           |
| `peer_cites`, `peer_papers`, `peer_rate` | the same, over the other seats of the edition      |
| `expected`         | `max(rate, peer_rate) * papers`, the citations expected                  |
| `surplus`          | `cites - expected`                                                       |
| `expected_citing`  | the same as `expected`, for `citing`: the citing papers expected         |
| `depth`            | `(cites / citing) / (expected / expected_citing)`: citations per citing paper over those expected |

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

The corpus keeps the seats with a rate and papers. A member is scored with at least `MIN_SEATS`
seats, and ranked when one of all their seats has more than `GATE_REFERENCES` references
(`cites_gate`).

The evidence of a seat is its state: the bin of its surplus (`STATES`, lower edges
`SURPLUS_EDGES`), and in every bin after `typical` whether it is deep, with `depth` at least
`DEPTH_EDGE`. Its genuine baseline, the state distribution of genuine seats, has two observed
parents: `src`, and a
band, of `expected` (`PROM_BANDS`) with `src = conference`, of `pubs` (`PUBS_BANDS`) with
`src = peers`. The baseline is the share of seats in each state with the same `src` and band, in
the seat's conference (stream, satellites included), else its CORE rank, else all conferences:
the first level that keeps `FALLBACK_MIN_N` seats after leaving out the scored member's, the last
regardless. Every state gets `PSEUDO_COUNT`. All seats in the corpus count in the baseline.

A coerced seat falls in a bin by `MANIP_DIST`, and is deep there with `MANIP_DEEP`. A seat in a
state adds `log LR`, with `LR = genuine / (ALPHA manip + (1 - ALPHA) genuine)`, the baseline and
the coerced share of that state, to the prior log-odds of `PRIOR`. `woe` is the sum in
log10. A member is shortlisted below `SHORTLIST_THRESHOLD`.

## Pipeline

`bbn_extract.py` builds the corpus in stages:

| Stage          | Reads                      | Builds                                                           |
|----------------|----------------------------|------------------------------------------------------------------|
| `citations`    | database, OpenCitations Index | `lncs_citation.well_known`, `citation` and `paper` (rebuilt)  |
| `seats`        | database                   | `bbn/seats.csv`, with the funnel and the gate count              |
| `publications` | dump, `bbn/seats.csv`      | `bbn/publications.csv`: publications per member and year         |
| `corpus`       | the two CSVs above         | `bbn/bbn_cs3_corpus.json`: the seats per member, with the funnel |

`bbn_infer.py` scores the corpus and writes `bbn/bbn_cs3_ranking.csv` (the gated members),
`bbn/bbn_cs3_ranking_ungated.csv` (every scored member) and `bbn/bbn_cs3_scored.json` (with the
evidence per seat). It prints the gated shortlist and the rank of the members in `WATCH_PIDS`.

```
python bbn_extract.py --stage citations      # one pass over the Index, or --index; --rescan
python bbn_extract.py --stage seats
python bbn_extract.py --stage publications   # one pass over the dump, or --dump
python bbn_extract.py --stage corpus
python bbn_infer.py
```

`citations` saves the Index counts in `bbn/index_counts.pkl` and reads them back on the next run
when the cited works and years are the same. After a new Index dump, pass `--rescan`.

Only `citations` and `seats` need the database; the rest runs on the standard library.
`test_bbn_extract.py` and `test_bbn_infer.py` run without data.
