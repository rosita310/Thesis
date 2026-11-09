# PC member citation BBN

Scores every PC member of an LNCS conference on P(genuine) with a Bayesian belief network over
their seats: per seat, the citations from the edition's papers against those expected from the
editions where the member had no role. All settings are in `cs3_config.py`.

## Input

- The database (connection in `solution/.env`): the parsed committees
  (`lncs_front_matter.committee_member`), the LNCS volumes, papers and authors and the cited
  works (`dblp_dump`), and the references (`opencitations_dump.reference`).
- The OpenCitations Index dump in `RAW_DATA/opencitations/index`, to leave out well-known works.
- The dblp dump, for the members' publications.

## Pipeline

| Step                                | Writes                                                      |
|-------------------------------------|-------------------------------------------------------------|
| `bbn_extract.py --stage citations`  | `lncs_citation.well_known`, `citation` and `paper`          |
| `bbn_extract.py --stage seats`      | `bbn/seats.csv`: one row per seat, with `expected` and `surplus` |
| `bbn_extract.py --stage publications` | `bbn/publications.csv`                                    |
| `bbn_extract.py --stage corpus`     | `bbn/bbn_cs3_corpus.json`                                   |
| `bbn_infer.py`                      | `bbn/bbn_cs3_ranking.csv` (gated), `bbn_cs3_ranking_ungated.csv`, `bbn_cs3_scored.json` |
| `bbn_inject.py`                     | `bbn/bbn_cs3_injection.csv`: how many planted coerced members are shortlisted |

`citations` caches the Index counts in `bbn/index_counts.pkl`; pass `--rescan` after a new Index
dump. Only `citations` and `seats` need the database.

## Tests

`test_bbn_extract.py`, `test_bbn_infer.py` and `test_bbn_inject.py` run without data.
