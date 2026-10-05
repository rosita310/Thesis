# LNCS volumes from the dblp dump

Builds `dblp_dump.lncs_volume`: every LNCS volume of the CORE conferences (ranks in `CORE_RANKS`)
with dblp key, year, DOI, LNCS volume number and number of papers in dblp.

The dblp API and website block scripted requests, so the volumes are read from the dblp RDF dump
(`solution/dblp.nt/dblp.nt`, or a `.nt.gz` via `--dump`). New dumps are published monthly at
https://drops.dagstuhl.de/entities/collection/dblp.

A conference is linked to its volumes through the dblp stream in the CORE `DBLP` column
(`https://dblp.uni-trier.de/db/conf/esorics` -> `conf/esorics`). A volume shared by two
conferences gets a row for each. The table is recreated on every run.

```
python lncs_volumes.py
```
