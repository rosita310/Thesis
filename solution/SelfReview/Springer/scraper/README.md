# Springer — Article Metadata Collection

Collects publication dates for articles in Springer Computer Science journals, to
analyse publication lag (submission → acceptance) as an indicator of possible
self-review fraud.

Dates needed per article: **Received** (= submitted), **Accepted**, **Published**.

## Layout

```
springer/
  journals_scraper.py       Step 1: list CS journals
  article_downloader.py     Step 2: scrape metadata from Springer HTML   — deprecated
  crossref_collector.py     Step 2b: same metadata via the Crossref API  — use this
  parse_output.py           Step 3: parse stored JSON into the database
  skip_journals.csv         Journals without received dates, skipped on later runs
  progress.json             Scraper progress (last completed page per journal)
  crossref_issn_map.csv     journal_id → ISSN, for the Crossref collector
  crossref_progress.json    Crossref collector progress, per journal
  .chrome_profile/          Chrome profile for the Selenium fallback (safe to delete)
  logs/                     Per-run log files
```

## Step 1 — Journals scraper

- Source: `link.springer.com/journals/browse-subject?subject=COMPUTER_SCIENCE`
- Selector: `h2.app-card-open__heading a[data-track-label]`
- Uses `requests` + `BeautifulSoup`; Selenium not needed here
- Writes `journal_id`, `name` to `springer.journals`

## Step 2 — Article downloader (deprecated)

> **No longer usable (2026-08-18).** `link.springer.com` serves a Fastly **client
> challenge** on every page, article and listing alike: HTTP 200, a short body with
> `<title>Client Challenge</title>` and assets under `/_fs-ch-<id>/`.
>
> The clearance is bound to the client's TLS fingerprint, not to a cookie — after a
> successful load in Chrome there is no `_fs_ch_*` cookie in the jar, and the
> challenge cookie `_fs_ch_st_*` expires within seconds. So the `requests` path
> cannot pass it and `_sync_cookies()` does not help: every following request is
> challenged again. Extra headers (`Accept`, `Sec-Fetch-*`, `sec-ch-ua`) change
> nothing.
>
> In practice every fetch falls through to Selenium. The data collected that way is
> still correct — the "Blocked" / "Block cleared" log lines are misleading, not
> wrong. Use step 2b instead.

- Walks each journal's article list via `/journal/{id}/articles?page={n}`, then
  extracts metadata per article page
- Stores `data/{journal_id}/{doi}.json`
- Stops paginating once it reaches an article older than `MIN_YEAR`
- Resumable per article (`already_downloaded()`) and per page (`progress.json`)
- Block detection is content-aware: a page counts as blocked only when the real
  content is absent *and* a full block-page string is present. Bare words like
  "captcha" are not matched, because they occur in legitimate article titles
- Journals that yield no received dates are appended to `skip_journals.csv`

### `progress.json`

Per journal: `status`, `last_page`, `articles_saved`, `received_count`, `min_year`,
`updated_at`. Written after each completed page.

`last_page` points at the last *fully* processed page. The page where `MIN_YEAR`
cuts in is only partly processed, so it is deliberately not counted — a later run
with a lower `MIN_YEAR` revisits it and picks up the older half, while
`already_downloaded()` skips what was saved before.

When lowering `MIN_YEAR`, set the affected journals back from `done` to
`in_progress` by hand but **keep `last_page`**: Springer lists newest first, so the
older articles are on the later pages.

## Step 2b — Crossref collector

Fetches the same metadata from the Crossref REST API instead of Springer HTML. No
browser, no challenge, no backoff. Writes the same JSON shape to the same
`data/{journal_id}/{doi}.json`, so `parse_output.py` runs unchanged.

```
python crossref_collector.py
```

`Received` and `Accepted` live in Crossref's `assertion` array. Validated
field-by-field against already-scraped articles: dates, authors, volume, pages and
ISSN match exactly.

### Springer only

Assertions are **voluntary publisher deposits**, not a standard Crossref field.
Springer deposits them; **Elsevier and IEEE deposit none at all**, so for those
publishers scraping remains the only route.

Note that the `search.crossref.org` UI does not display assertions — only the API
returns them. In the JSON the `assertion` array sits well below the date fields,
among licence and funder data.

### Three fields are left empty

Written as `null`/empty rather than approximated, so the database keeps NULL where
the value was not determined instead of mixing two vocabularies in one column:

| Field | Reason |
|---|---|
| `affiliations` | Crossref does not carry them for Springer |
| `article_type` | Crossref only ever reports `"journal-article"`; the scraper captured granular values like "Original Article" |
| `open_access` | Springer attaches its TDM licence to paywalled articles too, so the `license` array is not an open-access signal |

None of the three is read by analysis code — they appear only in the ingest schemas
of `parse_output.py`.

### Behaviour

- Pages per journal through `/journals/{issn}/works` using Crossref's **cursor**
  (deep paging), filtered on
  `type:journal-article,from-pub-date:{MIN_YEAR}-01-01`. The bulk listing carries
  the assertions, so no per-DOI request is needed.
- `CONTACT_EMAIL` goes into the User-Agent for Crossref's polite pool; without a
  contact address you land in the shared pool and hit 429s sooner. `Retry-After` is
  respected.
- **Never overwrites.** A DOI already present under `data/{journal_id}/` is skipped,
  so affiliation data in previously scraped files stays intact and a run only fills
  gaps. The check is case-insensitive.
- Each new file carries `"source": "crossref"`. `parse_output.py` reads named keys
  only, so it is ignored at ingest but distinguishes API rows from scraped rows in
  the JSON corpus.
- Partial Crossref dates (`published-print` is often year-and-month only) become
  `null` rather than a fabricated day.

### `crossref_issn_map.csv`

Crossref works on ISSN, but `springer.journals` holds only `journal_id` and `name`.
The mapping is derived automatically from the `issn` in already-scraped JSON files
(the scraper captured the electronic ISSN; Crossref accepts either variant and
returns identical results). Hand-edited values always win.

Journals with no scraped data get an empty `issn` and are **skipped** with a
warning — deliberately not looked up by title, since a fuzzy match would silently
collect a different journal. Add the ISSN by hand to include them. Some entries are
legitimately empty: one is a book archive rather than a journal, and several are
recent launches that Crossref does not index as journals.

### `crossref_progress.json`

Separate from `progress.json` so the two collectors never overwrite each other's
state. Per journal: `status`, `articles_written`, `already_present`,
`received_count`, `min_year`, `issn`, `updated_at`.

A journal recorded at a higher `min_year` is re-run automatically, so unlike
`progress.json` there is no need to reset anything by hand after lowering
`MIN_YEAR`. Resuming needs no stored cursor: completed articles are recognised by
file existence, and re-walking a journal costs only a few requests.

## Stored JSON format

```json
{
  "doi": "10.1007/s10015-026-01123-8",
  "title": "...",
  "journal_id": "10015",
  "received": "2025-08-31",
  "accepted": "2026-03-01",
  "published": "2026-04-27",
  "fallback_date_label": null,
  "fallback_date_value": null,
  "authors": ["Kento Murata", "Shoichi Hasegawa"],
  "affiliations": [
    {
      "institution": "Ritsumeikan University, Osaka, Japan",
      "authors": ["Kento Murata", "Shoichi Hasegawa"]
    }
  ],
  "open_access": false,
  "article_type": "Original Article",
  "volume": "27",
  "first_page": "1",
  "last_page": "15",
  "issn": "1614-7456",
  "retrieved_at": "2026-05-14T16:07:09.767198"
}
```

`fallback_date_value` is filled only when none of received/accepted/published was
found, so it flags genuinely missing data instead of duplicating an issue date.

## Step 3 — Parse output

Reads the JSON files into four PostgreSQL tables:

| Table | Contents |
|---|---|
| `springer.articles` | One row per article (all flat fields) |
| `springer.authors` | One row per author, with position in the paper |
| `springer.affiliations` | One row per affiliation |
| `springer.affiliation_authors` | Affiliation ↔ author link table |

- Batched writes, flushed per `BATCH_SIZE` articles, all four tables in one
  transaction
- Resumable: DOIs already in `springer.articles` are loaded into a set at startup
  and skipped without reading the JSON
- `review_days` is computed at ingest as `accepted - received`
- Logs to terminal and to `logs/parse_output_<timestamp>.log`

## Technical notes

- Python 3.9, `requests`, `BeautifulSoup4`
- PostgreSQL via the shared `Postgress`/`Saver` library
- Only the needed fields are stored as small per-article JSON files, rather than
  full HTML
- Collection window is set by `MIN_YEAR`
- Selenium is only a fallback for the (now permanent) Springer block, lazily
  imported. Requires Selenium ≥ 4.6 so its own driver manager fetches a matching
  chromedriver (`pip install -U selenium`); otherwise it falls back to
  `webdriver-manager` if installed, and exits cleanly with instructions if neither
  is available
