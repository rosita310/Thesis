# LNCS front matter parser

Parses the LNCS front matter PDFs downloaded by `../lncs/front_matters_download.py` and stores the
committee members in the schema `lncs_front_matter`. Based on the parser of Ewoud Westerbaan
(`solution/pdf_parser`) as improved by Wibren Wiersma (Radboud University).

Changes compared to Wibren Wiersma's version:

- Reads `solution/.env` (or a properties file given as first argument) instead of `../config`.
- Word separation also uses pdfbox's threshold (0.3 of the average character width), so tightly set
  names such as `Baiying Lei` are no longer glued together.
- Sections are no longer lost on lines with more columns than the line that set the layout
  (e.g. names under a role heading), on rows without an affiliation, on rows with empty cells,
  on single-row sections, on names ending in a comma or on sections with more than three columns.
- Fonts without a font descriptor no longer make the whole file fail.
- Roles found per row by `Two_Role_NameAff` are kept instead of being overwritten by the section title.
- Volumes without an organisation section (e.g. workshop volumes with a committee per workshop)
  are parsed from their committee, chair, reviewer and referee sections.
- Names with dot leaders (`Eli Biham . . . . Technion`) and affiliations in parentheses that wrap to
  the next line (`Kousha Etessami (University of Edinburgh`) are split into name and affiliation.
- Members with an empty name are not stored.

## Output

- `file`: one row per PDF and run, with status `SUCCEEDED` or `FAILED`.
- `section`: every section parsed below the organisation section, with the parser that was chosen.
- `member`: the people found, with `role` (section title), name, first name, last name and affiliation.

Every run gets a new `run_id`; earlier runs stay in the tables. The filename is the dblp key with
`/` replaced by `_`, which links a file to `dblp_dump.lncs_volume`.

Sections other than committees (preface, sponsors, keynotes) are parsed as well, so select members
on `role` before using them. Conferences published in several volumes repeat the same front matter
in every volume, so deduplicate per conference edition (`stream`, `year` in `dblp_dump.lncs_volume`).

## Running

Requires a JDK 13 or newer. From this folder, with Maven:

```
mvn compile exec:java -Dexec.mainClass=Program
```

Or open the folder in VS Code with the Java extensions and run `Program`. A log per PDF is written to `log/`.
