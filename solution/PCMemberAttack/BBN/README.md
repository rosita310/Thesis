# PC member citation BBN

Scores every PC member of an LNCS conference on P(genuine) with a Bayesian belief network over
their seats. A seat is suspect when the papers of that edition cite the member more than the
member's citation rate in editions where they hold no role predicts.

All settings are in `cs3_config.py`; the scripts take only paths on the command line.


## Input

All from the database, with the connection from `solution/.env`:

- `lncs_front_matter.committee_member`: the parsed committees, one row per person and role per
  edition, with the matched dblp pid.
- `dblp_dump.lncs_volume`, `lncs_article` and `lncs_article_author`: the LNCS volumes, their
  papers and authors. An edition is a volume key without its `-N` suffix, so the volumes of one
  conference year share one committee.
- `opencitations_dump.reference`: the references of the LNCS papers.
- `dblp_dump.cited_publication` and `cited_publication_author`: the cited works and their authors.

## Citations

A reference resolves to a dblp work through any of its DOIs (`cited_dois`), and counts once for
every author of that work. A citation is a self-citation when the cited author also wrote the
citing paper. References to dblp records without authors (proceedings, edited books) drop out.

## Seats

A seat is a member with a pid in the role `pc` or `senior_pc` of one edition. Chairs and
reviewers hold no scored seat. Per seat:

| Column         | Meaning                                                                    |
|----------------|----------------------------------------------------------------------------|
| `x`            | references to the member from the edition, the member's own papers left out |
| `x_westerbaan` | references to the member from the edition, self-citations included         |
| `n`            | papers of the edition with a reference resolved to dblp, the member's own left out |
| `k_off`, `n_off` | the same `x` and `n`, summed over the member's off-power editions, or over the peers |
| `src`          | where the rate comes from: `conference` or `peers`                         |
| `span`         | with `conference`: years between the seat and the furthest edition taken   |
| `rate`         | `(k_off + RATE_PSEUDO_COUNT) / n_off`                                      |
| `E`            | `rate * n`, the citations the rate predicts                                |
| `ratio`        | the citation ratio `(x + RATIO_PSEUDO_COUNT) / (E + RATIO_PSEUDO_COUNT)`: observed over expected citations |

An edition is off-power for a member when they hold no role of any kind there, also not under an
unmatched name they carry elsewhere, and when its parsed committee is plausibly complete
(`MIN_PC_SIZE`, `MIN_PC_SHARE`). Otherwise a member the parser missed would look off-power. Only
main editions (`conf/cpm/2007`) are off-power editions, never a satellite (`conf/eccv/2020w`,
`conf/eccv/2024-w8`), which has its own committee and papers. Years are the conference's, from
the edition key, not those of publication.

The off-power editions of a seat are those of its stream within `RATE_WINDOW` years, and, while
they hold fewer than `MIN_OFF_PAPERS` papers, further out nearest first (editions equally far
together). A satellite seat takes the rate of its main conference. When the whole stream has too
few, the rate is that of the other seats of the edition (Westerbaan's comparison), if there are
at least `MIN_PC_SIZE - 1` of them. A seat without either has no rate.

The gate is Westerbaan's: a member is ranked when at least one seat has more than
`GATE_REFERENCES` references (`x_westerbaan`).

## Pipeline

`bbn_extract.py` builds the corpus in stages:

| Stage       | Reads    | Builds                                                         |
|-------------|----------|----------------------------------------------------------------|
| `citations` | database | `lncs_citation.citation` and `lncs_citation.paper` (rebuilt)   |
| `seats`     | database | `bbn/seats.csv`, with the funnel and the gate count            |

```
python bbn_extract.py --stage citations
python bbn_extract.py --stage seats
```

Both stages need the database. `test_bbn_extract.py` runs without data.
