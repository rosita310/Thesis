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

## Cited publications

`cited_publications.py` builds, for the DOIs cited in `opencitations_dump.reference`
(see `../opencitations`), `dblp_dump.cited_publication` (dblp key, DOI, title, year, type, venue)
and `dblp_dump.cited_publication_author` (same columns as `lncs_article_author`). Cited works that
are not in dblp are not included.

## Persons

`persons.py` builds `dblp_dump.person_name`: one row per dblp person and name (the primary name and
every alias), with `is_primary`, the ORCID and the primary affiliation dblp lists for the person
(their current one, not the one at the time of a paper).

```
python lncs_volumes.py
python lncs_articles.py
python cited_publications.py    # after ../opencitations/references.py
python persons.py
```
