# LNCS Front matter download script

Downloads the front matter PDF of every LNCS volume in `dblp_dump.lncs_volume`
(see `../../dblp_dump`) for the CORE ranks in `CORE_RANKS`, published from `MIN_YEAR` onwards.

Per volume it opens the Springer book page via the DOI, takes the front matter link
(`/content/pdf/bfm:<ISBN>/1`) and stores the PDF as `<dblp key with / replaced by _>.pdf`
in `RAW_DATA/LNCS_FRONT_MATTER_SUBDIR` (from `.env`), or in `--output-dir`.

Every attempt is logged in `springer_lncs.download_process_info`. A rerun skips volumes with
status `SUCCEEDED` or `NO_FRONT_MATTER` and retries `FAILED` ones. When Springer serves a bot
challenge or keeps rate limiting, the script stops; rerun later to continue.

```
python download.py --limit 10
```
