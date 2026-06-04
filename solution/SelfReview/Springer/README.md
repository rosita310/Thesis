# Springer Scraper

## Doel
Publicatiedata verzamelen van artikelen in Springer Computer Science journals om de publication-lag (tijd tussen indiening en acceptatie) te analyseren als indicator voor mogelijke self-review fraude.

## Benodigde datums per artikel
- **Received** (= submitted)
- **Accepted**
- **Published**

## Architectuur

Drie Python-scripts, vergelijkbaar met de Elsevier aanpak:

```
springer/
  journals_scraper.py     Stap 1: lijst van CS journals ophalen        ✅
  article_downloader.py   Stap 2: per journal artikelen ophalen en metadata opslaan  ✅
  parse_output.py         Stap 3: opgeslagen JSON parsen en naar database schrijven  ✅
  skip_journals.csv       Journals zonder received-datum automatisch overslaan
  progress.json           Voortgang per journal (laatste voltooide pagina, status)
  .chrome_profile/        Persistent Chrome-profiel voor de Selenium-fallback (mag weg)
  logs/                   Logbestanden per run (parse_output_YYYYMMDD_HHMMSS.log)
```

## Stap 1 — Journals scraper ✅
- Bron: `link.springer.com/journals/browse-subject?subject=COMPUTER_SCIENCE`
- Pagineert automatisch (20 per pagina, 8 pagina's)
- Scraper gebruikt `requests` + `BeautifulSoup` (Selenium niet nodig gebleken)
- Selector: `h2.app-card-open__heading a[data-track-label]`
- Resultaat: **143 journals** opgeslagen in `springer.journals` (PostgreSQL)
- Kolommen: `journal_id`, `name`

## Stap 2 — Article downloader ✅
- Per journal: artikellijst doorlopen via `/journal/{id}/articles?page={n}` (50 per pagina)
- Per artikel: metadata extraheren uit de artikelpagina
- Opgeslagen als kleine JSON-bestanden op schijf: `data/{journal_id}/{doi}.json`
- Artikelen van vóór `MIN_YEAR` worden overgeslagen (stopconditie op publicatiejaar)
- Script is veilig hervattbaar op twee niveaus:
  - **Per artikel**: `already_downloaded()` check (vangnet op paginagrenzen)
  - **Per journal/pagina**: `progress.json` (zie hieronder) — voltooide journals worden
    volledig overgeslagen en een onderbroken journal hervat op de volgende pagina i.p.v.
    alle paginavanaf 1 opnieuw op te halen
- **Hybride fetch + handmatige CAPTCHA-oplossing**: standaard snel via `requests`; bij een
  block/CAPTCHA schakelt het script automatisch over naar een zichtbaar Chrome-venster
  (Selenium). Daar los je de CAPTCHA met de hand op, waarna het verdergaat en de
  browser-cookies terugkopieert naar de `requests`-sessie (terug naar het snelle pad).
- **Adaptieve snelheid**: lage basisdelay (0.25–1.0s) voor doorvoer, met automatische
  backoff (×2 per block, gedempt bij succes) zodat het script vertraagt zodra Springer
  begint te blokkeren en weer versnelt als het rustig is.
- 429-afhandeling (60s wacht) en CAPTCHA/block-detectie blijven behouden.
- Journals zonder received-datum worden automatisch toegevoegd aan `skip_journals.csv`

### Voortgang bijhouden — `progress.json`
Per journal wordt opgeslagen: `status` (`in_progress`/`done`), `last_page` (laatste
volledig voltooide paginalijst), `articles_saved`, `received_count`, `min_year`
(de drempel die een `done`-status weerspiegelt) en `updated_at`. Het bestand wordt na
elke voltooide pagina atomisch weggeschreven, dus een block kost hooguit één pagina.

**Iteratief scrapen (2020 → 2010 → 2000)**: verlaag je `MIN_YEAR`, zet dan de betrokken
journals in `progress.json` met de hand terug van `"done"` naar `"in_progress"` maar
**behoud `last_page`**. Omdat Springer nieuw-naar-oud lijst, staan de oudere artikelen op
de látere pagina's; de run hervat op `last_page + 1` en gaat direct door met de nieuwe data
zonder al opgehaalde pagina's opnieuw te bezoeken. Het `min_year`-veld laat zien tot welke
drempel elke `done` is voltooid.

### Formaat van opgeslagen JSON
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

## Stap 3 — Parse output ✅
- JSON-bestanden inlezen en schrijven naar vier PostgreSQL-tabellen:

| Tabel | Inhoud |
|---|---|
| `springer.articles` | Één rij per artikel (alle platte velden) |
| `springer.authors` | Één rij per auteur, met positie (volgorde in paper) |
| `springer.affiliations` | Één rij per affiliatie met institutienaam |
| `springer.affiliation_authors` | Koppeltabel affiliatie ↔ auteur |

- **Batch-schrijven**: elke 500 artikelen worden geflushed naar de DB (geheugengebruik laag, DB-round-trips beperkt)
- **Hervattbaar**: bij opstarten worden alle DOIs uit `springer.articles` geladen in een set; al verwerkte bestanden worden overgeslagen zonder de JSON te lezen
- **Logging**: tegelijk naar terminal en naar `logs/parse_output_YYYYMMDD_HHMMSS.log`
- Tabel- en kolomaanmaak volledig automatisch via de gedeelde `Saver`-library

## Opslagschatting
| Aanpak | Grootte |
|---|---|
| Volledige HTML opslaan | ~130 GB |
| Alleen metadata als JSON | ~400 MB |

Door alleen de benodigde velden op te slaan blijft de totale schijfruimte ruim onder de 1 GB voor de verwachte ~400.000 artikelen (143 journals, 2000–heden).

## Technische keuzes
- **Taal**: Python 3.9 (`requests`, `BeautifulSoup4`)
- **Database**: PostgreSQL via gedeelde `Postgress`/`Saver` library (pyodbc)
- **Selenium**: alleen als fallback bij een block/CAPTCHA (niet voor normale fetches —
  Springer-pagina's zijn server-side rendered). Gewone `selenium` met de ingebouwde
  Selenium Manager (haalt zelf de juiste chromedriver op); lazy geïmporteerd, dus alleen
  nodig zodra er daadwerkelijk geblokkeerd wordt. Installeren: `pip install selenium`.
  Een hard IP-block lost de browser niet op — val dan terug op de schone hervatting
  (progress + opnieuw starten vanaf een ander netwerk/VPN).
- **Tussenopslag**: minimale JSON-bestanden per artikel — biedt robuustheid zonder opslagprobleem
- **Periode**: iteratief, instelbaar via `MIN_YEAR` (eerst 2020, daarna 2010, uiteindelijk 2000)
