# Committee members per conference edition

`committee_members.py` builds `lncs_front_matter.committee_member` from the parsed front matter
(`lncs_front_matter.member`, latest run) and the dblp tables in `dblp_dump`.

## Deduplication

Conferences published in several volumes repeat the same front matter in every volume. A member is
stored once per edition: the dblp key of the volume without its volume number
(`conf/eccv/2020-1` ... `conf/eccv/2020-12` -> `conf/eccv/2020`). Workshop volumes keep their own
key (`conf/eccv/2014w1`). Names are compared on `name_key`: lower case, without accents,
punctuation and dblp homonym numbers. The printed name, role and affiliation are the most frequent
ones over the volumes; `cnt_volumes` counts the volumes the member appears in.

## Role categories

The section title (`role`) is mapped to `role_category`, first match wins:

| Category    | Examples                                                          |
|-------------|-------------------------------------------------------------------|
| `other`     | prefaces and welcome messages (parsed as sections, not people)    |
| `pc_chair`  | Program Chairs, PC Co-chairs, Technical Program Chair             |
| `senior_pc` | Senior Program Committee, Area Chairs, Track Chairs, Meta-reviewers |
| `pc`        | Program Committee, Programme Commitee, Technical Program Committee, Track A |
| `reviewer`  | Additional/External Reviewers, Referees, Subreviewers             |
| `other`     | everything else: general chairs, steering committee, sponsors, ...  |

## Link to dblp

A member is linked to a dblp person (`dblp_pid`) by name. The name is tried in this order
(`match_rule`):

1. `exact`: the name as printed.
2. `without_country`: without a leading country, which the parser sometimes takes from the line
   above (`India Naveen Prakash`).
3. `without_initials` / `without_country_initials`: without single letters on both sides, so
   `Michael Wooldridge` finds `Michael J. Wooldridge`. Only when two full name parts remain.

Each variant is looked up first among the authors of the LNCS papers of the same conference
(`match_status = matched_stream`), then among all dblp persons with all their alias names
(`matched_dblp`, from `dblp_dump.person_name`). The first lookup that finds anyone decides: one
person is a match, several persons with that name is `ambiguous_*` and no person is chosen.
Members no variant finds are `not_found`; these are mostly parser noise (affiliations or
organisations read as a name) and people dblp lists under a different name.

```
python ../dblp_dump/persons.py    # once per dblp dump
python committee_members.py
```
