# Co-author editor BBN

Scores every editor on P(genuine) with a Bayesian belief network over their co-authorships on
papers in the journal they edit. A paper is suspect when the front matter of its own issue lists
one of its authors on the board. ACM and IEEE journals are covered.

All settings are in `cs2_config.py`; the scripts take only paths on the command line.

## The network

Each co-authorship of an editor E with a co-author A in journal j is one node with a load (one
suspect paper carries load 1, split over its co-authors) and a tie: `outside_5plus`,
`outside_2_4`, `outside_1` (papers E and A share outside j), `repeated` or `single` (only inside
j). The network is G -> tie <- A's one-off habit, with the habit observed. A node adds
load * log LR to E's log-odds, with LR = g(t) / (alpha m(t) + (1 - alpha) g(t)): m(t) is the
elicited tie distribution of a coerced co-authorship, g(t) the genuine one.

g(t) is journal j's tie distribution without E's own nodes, smoothed, with a fallback to the
publisher's pool below `FALLBACK_MIN_N` (N_min) of load. For a co-author with at least N_min
collaborators of their own (outside E and the node's own papers), g is made personal: A's
one-off share (k + 0.5) / (n + 1), of n collaborators k with whom A shares one publication only,
over r_bar_j, the journal's load-weighted typical share (again without E, with the same
publisher fallback), gives rho, g(single) becomes g(single | j) * rho (at most
`PERSONAL_SINGLE_CAP`) and the other ties are rescaled to sum to 1.

## Input

- Per publisher in `PUBLISHERS`, a schema with `front_matter_issues` and `front_matter_board`:
  `acm`, filled by `../ACM/parse_json_to_DB.py`, and `ieee`, filled by `../IEEE/parse_json_to_DB.py`.
  A publisher whose schema is missing is left out with a warning. The connection comes from
  `solution/.env`.
- The dblp RDF dump at `solution/dblp.nt/dblp.nt` (or `--dump`).

## Publishers

All publishers form one corpus. An editor is one person across publishers: their co-authorships
in ACM and IEEE journals add up to one P(genuine), and the in/out gate and the strict `outside`
reading see all their board seats. The genuine baselines stay per publisher: a journal's own
baseline, the pooled fallback over the journals of the same publisher, and the same for the
typical one-off share r_bar. An editor on the boards of one publisher gets the same score as in a
corpus of that publisher alone.

A front-matter issue is joined to dblp on volume and issue for ACM, and on year and issue for
IEEE. Parts of one IEEE issue (`2_Part_1`, `2_Part_2`) map to the same dblp issue and are merged.
A board name matches a signer when the names agree ignoring case, accents and punctuation, or,
failing that, on the surname with a compatible first initial (`W. R. STONE` and `W. Ross Stone`).

## Pipeline

`dblp_extract.py` reads the articles of our journals and their authors from the dump into
`reports/extract/`, and proposes the mapping from journal names to dblp journal keys. Rows of an
existing `reports/journal_mapping.csv` are carried over as `verified`. Check every other row of
`reports/journal_mapping_proposed.csv` whose confidence is not `high` and save it as
`reports/journal_mapping.csv`.

`bbn_extract.py` builds the corpus in `bbn/`, in stages:

| Stage            | Reads              | Builds                                                           |
|------------------|--------------------|------------------------------------------------------------------|
| `pairs`          | database, extract  | suspect papers, co-authorships, board tenure, pre-tenure control |
| `coauthors`      | dump               | every publication of the editors and co-authors                  |
| `years`          | dump               | the year of each of those publications (co-author's career stage) |
| `editor_authors` | dump               | authors and year of the editors' publications (in/out gate)      |
| `signer_authors` | dump               | authors of the signers' publications (co-authors' own networks)  |
| `corpus`         | the files above    | `bbn_cs2_corpus.json`                                            |

The dump stages are cached; `--force` redoes them.

`bbn_infer.py` scores the corpus and writes `bbn/bbn_cs2_ranking.csv` and
`bbn/bbn_cs2_scored.json`, with the publishers of each editor. `bbn_report.py` prints the
diagnostics and sensitivity sweeps, and `bbn_placebo.py` runs the within-paper placebo tests and
writes `bbn/bbn_cs2_placebo.csv`. `bbn_inject.py` plants coerced papers in one editor's record at
a time, with the other editors and the baselines unchanged, and writes how often the model then
shortlists them to `bbn/bbn_cs2_injection.csv`, per record band and for editors on two or more
boards (who also get the coerced papers spread over two seats); a planted co-authorship copies
the career stage and pool stats of a real co-authorship of the same tie in that journal, so it
gets the personal adjustment a real co-author of that tie gets.

```
python ../IEEE/parse_json_to_DB.py         # once, and after new IEEE front matter
python dblp_extract.py --stage extract
python dblp_extract.py --stage discover    # then verify the mapping
python bbn_extract.py --stage all
python bbn_infer.py
python bbn_report.py
python bbn_placebo.py
python bbn_inject.py
```

Only the two loaders, `dblp_extract.py` and `bbn_extract.py --stage pairs` need the database; the
rest runs on the standard library. `dblp_extract.py` and each `bbn_*.py` have a `test_*.py` that
runs without data.
