# PC member citation BBN

Scores every PC member of an LNCS or ACM conference on P(genuine) with a Bayesian belief network
over their seats: per seat, the citations from the edition's papers against those expected from
the editions where the member had no role. The editions of both publishers are taken together
(`SOURCES`), so all seats of a member count. All settings are in `cs3_config.py`.

## Input

- The database (connection in `solution/.env`), per source: the committees
  (`lncs_front_matter.committee_member`, `acm_front_matter.committee_member`), the volumes, papers
  and authors (`dblp_dump.lncs_*`, `dblp_dump.acm_*`) and the references
  (`opencitations_dump.reference`, `acm_reference`); the cited works (`dblp_dump.cited_publication`).
  See `../dblp_dump`, `../opencitations` and `../integration` for how they are built, with
  `--source acm` for ACM.
- The OpenCitations Index dump in `RAW_DATA/opencitations/index`, to leave out well-known works
  and to find where the citing authors cite a work elsewhere; the OMID index in
  `RAW_DATA/opencitations/omid`, for the OMIDs of their publications.
- The dblp dump, for the members' publications and those of the citing authors.

## Pipeline

| Step                                | Writes                                                      |
|-------------------------------------|-------------------------------------------------------------|
| `bbn_extract.py --stage citations`  | `cs3_citation.edition_map`, `well_known`, `citation` and `paper` |
| `bbn_extract.py --stage authors`    | `cs3_citation.checkable` and `explained`                    |
| `bbn_extract.py --stage seats`      | `bbn/seats.csv`: one row per seat, with `expected` and `surplus` |
| `bbn_extract.py --stage publications` | `bbn/publications.csv`                                    |
| `bbn_extract.py --stage corpus`     | `bbn/bbn_cs3_corpus.json`                                   |
| `bbn_infer.py`                      | `bbn/bbn_cs3_ranking.csv` (gated), `bbn_cs3_ranking_ungated.csv`, `bbn_cs3_scored.json` |
| `bbn_inject.py`                     | `bbn/bbn_cs3_injection.csv`: how many planted coerced members are shortlisted |
| `bbn_report.py`                     | prints the shortlist under every variant of `SENSITIVITY_VARIANTS`, one setting changed at a time |

`citations` caches the Index counts in `bbn/index_counts.pkl`; pass `--rescan` after a new Index
dump. `citations`, `authors` and `seats` need the database. With `CHECK_CITING_AUTHORS` off,
`seats` counts every citation and paper and `authors` can be skipped.

## Tests

`test_bbn_extract.py`, `test_bbn_infer.py`, `test_bbn_inject.py` and `test_bbn_report.py` run
without data.
