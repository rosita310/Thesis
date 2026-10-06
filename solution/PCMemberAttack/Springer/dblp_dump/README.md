# LNCS volumes and papers from the dblp dump

The dblp API and website block scripted requests, so everything is read from the dblp RDF dump
(`solution/dblp.nt/dblp.nt`, or a `.nt.gz` via `--dump`). New dumps are published monthly at
https://drops.dagstuhl.de/entities/collection/dblp. Both scripts recreate their tables on every run.

## Volumes

`lncs_volumes.py` builds `dblp_dump.lncs_volume`: every LNCS volume of the CORE conferences
(ranks in `CORE_RANKS`) with dblp key, year, DOI, LNCS volume number and number of papers in dblp.

A conference is linked to its volumes through the dblp stream in the CORE `DBLP` column
(`https://dblp.uni-trier.de/db/conf/esorics` -> `conf/esorics`). A volume shared by two
conferences gets a row for each.

## Papers

`lncs_articles.py` builds, for the volumes in `dblp_dump.lncs_volume`:

- `dblp_dump.lncs_article`: one row per paper with dblp key, volume, DOI, title, year and pages.
- `dblp_dump.lncs_article_author`: one row per author of a paper, in author order, with the dblp
  person id (`dblp_pid`), the dblp name (homonyms carry a number, e.g. `Yang Cao 0011`), the name
  without that number and the ORCID when dblp has one.

Author affiliations are not in dblp (nor in Crossref for LNCS chapters).

```
python lncs_volumes.py
python lncs_articles.py
```
