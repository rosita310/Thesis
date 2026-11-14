# LNCS and ACM volumes and papers from the dblp dump

The dblp API and website block scripted requests, so everything is read from the dblp RDF dump
(`solution/dblp.nt/dblp.nt`, or a `.nt.gz` via `--dump`). New dumps are published monthly at
https://drops.dagstuhl.de/entities/collection/dblp. The scripts recreate their tables on every run.

`volumes.py` and `articles.py` take `--source lncs` (the default) or `--source acm`; the
ACM tables are named like the LNCS ones with `acm_` in place of `lncs_`.

## ACM volumes

`volumes.py --source acm` builds `dblp_dump.acm_volume`: the dblp proceedings of the same
CORE conferences that get an ACM front matter from `PCMemberAttack/ACM/data` (`--acm-data`).
Instead of the LNCS volume number it has `front_matter`, the JSON file. LNCS volumes, which the
ACM DL also lists, are left out: they come from the LNCS source.

A front matter is linked to a volume by its ISBN (from the file name, compared as ISBN-13). The
ACM DL reuses some ISBNs for other proceedings, so:

1. A file whose name has a year (`AAMAS '08`, `CATS 2011`) is a candidate only for a volume of
   that year (its dblp year or the year in its key).
2. Several candidates for one volume are narrowed to those whose acronym (the first word of the
   name) is the conference's, its dblp stream or CORE acronym. A volume still without exactly one
   candidate gets no front matter.
3. A file left with volumes in several editions (`conf/splc/2024a` and `2024b`) keeps one: the
   edition whose acronym is the file's, else the one with the most papers.

Files without an ISBN in their name (`..._Unknown_ISBN.json`) cannot be linked; the script logs
how many.

## Volumes

`volumes.py` builds `dblp_dump.lncs_volume`: every LNCS volume of the CORE conferences
(ranks in `CORE_RANKS`) with dblp key, year, DOI, LNCS volume number and number of papers in dblp.

A conference is linked to its volumes through the dblp stream in the CORE `DBLP` column
(`https://dblp.uni-trier.de/db/conf/esorics` -> `conf/esorics`). A volume shared by two
conferences gets a row for each.

## Papers

`articles.py` builds, for the volumes in `dblp_dump.lncs_volume`:

- `dblp_dump.lncs_article`: one row per paper with dblp key, volume, DOI, title, year and pages.
- `dblp_dump.lncs_article_author`: one row per author of a paper, in author order, with the dblp
  person id (`dblp_pid`), the dblp name (homonyms carry a number, e.g. `Yang Cao 0011`), the name
  without that number and the ORCID when dblp has one.

Author affiliations are not in dblp (nor in Crossref for LNCS chapters).

## Cited publications

`cited_publications.py` builds, for the DOIs cited in `opencitations_dump.reference` and
`acm_reference` (see `../opencitations`), `dblp_dump.cited_publication` (dblp key, DOI, title, year, type, venue)
and `dblp_dump.cited_publication_author` (same columns as `lncs_article_author`). Cited works that
are not in dblp are not included.

## Persons

`persons.py` builds `dblp_dump.person_name`: one row per dblp person and name (the primary name and
every alias), with `is_primary`, the ORCID and the primary affiliation dblp lists for the person
(their current one, not the one at the time of a paper).

```
python volumes.py [--source acm]
python articles.py [--source acm]
python cited_publications.py    # after ../opencitations/references.py for both sources
python persons.py
```
