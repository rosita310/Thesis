# Co-author editor BBN

Scores every editor on P(genuine) with a Bayesian belief network over their co-authorships on
papers in the journal they edit. A paper is suspect when the front matter of its own issue lists
one of its authors on the board. ACM and IEEE journals are covered.

All settings are in `cs2_config.py`; the scripts take only paths on the command line.


## Input

- Per publisher in `PUBLISHERS`, a schema with `front_matter_issues` and `front_matter_board`:
  `acm`, filled by `../ACM/parse_json_to_DB.py`, and `ieee`, filled by `../IEEE/parse_json_to_DB.py`.
  A publisher whose schema is missing is left out with a warning. The connection comes from
  `solution/.env`.
- The dblp RDF dump at `solution/dblp.nt/dblp.nt` (or `--dump`).

## Publishers

A front-matter issue is joined to dblp on volume and issue for ACM, and on year and issue for
IEEE. Parts of one IEEE issue (`2_Part_1`, `2_Part_2`) map to the same dblp issue and are merged.
A board name matches a signer when the names agree ignoring case, accents and punctuation, or,
failing that, on the surname with a compatible first initial (`J. A. DOE` and `J. Aron Doe`).

## Pipeline

`dblp_extract.py` reads the articles of our journals and their authors from the dump into
`reports/extract/`, and proposes the mapping from journal names to dblp journal keys. Rows of an
existing `reports/journal_mapping.csv` are carried over as `verified`. Check every other row of
`reports/journal_mapping_proposed.csv` whose confidence is not `high` and save it as
`reports/journal_mapping.csv`.

`bbn_extract.py` builds the corpus in `bbn/`, in stages:

| Stage            | Reads              | Builds                                                           |
|------------------|--------------------|------------------------------------------------------------------|
| `pairs`          | database, extract  | suspect papers, co-authorships, board terms, pre-board control   |
| `coauthors`      | dump               | every publication of the editors and co-authors                  |
| `editor_authors` | dump               | authors of the editors' publications, year of all of the above   |
| `signer_authors` | dump               | authors of the signers' publications (co-authors' own networks)  |
| `corpus`         | the files above    | `bbn_cs2_corpus.json`                                            |

The dump stages are cached; `--force` redoes them.

`bbn_infer.py` scores the corpus and writes `bbn/bbn_cs2_ranking.csv` and
`bbn/bbn_cs2_scored.json`, with the publishers of each editor. `bbn_report.py` prints the
diagnostics and sensitivity sweeps, and `bbn_placebo.py` runs the within-paper placebo tests and
writes `bbn/bbn_cs2_placebo.csv`. `bbn_inject.py` makes fake editors who built a career on
coercion; how often they fall below 0.5 and their median rank go to `bbn/bbn_cs2_injection.csv`.

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
