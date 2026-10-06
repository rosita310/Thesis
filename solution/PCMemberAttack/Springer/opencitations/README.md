# Citations from OpenCitations

Loads the citations made by the LNCS papers in `dblp_dump.lncs_article` into
`opencitations_dump.reference` (`citing` and `cited` are DOIs, as in the original pipeline).
A cited work can have several DOIs (e.g. publisher and repository); `cited` holds one of them and
`cited_dois` all of them, space separated, so match on `cited_dois`.

The OpenCitations Index dump identifies papers by OMID (`omid:br/...`), not by DOI. The OMID index
(`meta_br`) maps DOIs to OMIDs. Only citations with a DOI on both sides are linked; references
without a DOI are not in OpenCitations. Papers missing from the OMID index are missing from
OpenCitations itself (checked against the REST API).

Data (versions are pinned in `download.py`):

- OpenCitations Index, citation data CSV: https://doi.org/10.6084/m9.figshare.24356626 (version 8)
- OMID index: https://doi.org/10.6084/m9.figshare.24427156 (version 4)

The files are stored in `RAW_DATA/opencitations` (from `.env`); the zips are read without unpacking.

```
python download.py
python references.py
```

`download.py` resumes interrupted downloads and checks file sizes. `references.py` scans the index
files in parallel (`--processes`) and recreates the table. Afterwards, `../dblp_dump/cited_publications.py`
loads the dblp records and authors of the cited DOIs.
