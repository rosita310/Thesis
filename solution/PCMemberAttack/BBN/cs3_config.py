"""
Settings of the PC member citation BBN pipeline, grouped by the script that uses
them. The scripts take only paths on the command line.
"""

import math
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent

ENV_PATH = BASE_DIR / ".." / ".." / ".env"
DUMP_PATH = BASE_DIR / ".." / ".." / "dblp.nt" / "dblp.nt"
BBN_DIR = BASE_DIR / "bbn"              # bbn_extract.py output, and everything after it

# ---------------------------------------------------------------------------
# Corpus construction (bbn_extract.py)
# ---------------------------------------------------------------------------

SOURCES = ["lncs", "acm"]    # the publishers whose editions are taken together
SCHEMA = "cs3_citation"      # the materialised citation and paper tables

ROLES = ["pc", "senior_pc"]  # the role categories whose seats are scored

# A member's career starts in the first year with one of CAREER_ROLES in any stream; only
# editions from that year on are off-power.
CAREER_ROLES = ["pc", "senior_pc", "pc_chair"]

# Editions of one conference and year whose matched pc + senior_pc overlap by at least this
# much (Jaccard) list the same committee in several volumes, and are taken as one edition.
SAME_COMMITTEE = 0.8

# The off-power rate: the stream's editions in which the member has no role at all, all
# those within RATE_WINDOW years, and further out nearest first, up to MAX_SPAN years, while
# they hold fewer than MIN_OFF_PAPERS papers. Too few falls back to the other seats of the
# edition. The expected citations are the seat's papers times the larger of that rate and the
# rate of the other seats.
RATE_WINDOW = 2
MAX_SPAN = 5
MIN_OFF_PAPERS = 30
RATE_PSEUDO_COUNT = 0.5      # added to the off-power citations

# A work is well known at an edition when its citations in the OpenCitations Index up to and
# including the edition's year, the authors' own left out, are in the top WELL_KNOWN_TOP of
# the works the stream cites that have a citation by then. A stream with fewer of those than
# WELL_KNOWN_MIN_WORKS takes the works of all streams. References to a well-known work do not
# count in cites. A work younger than WELL_KNOWN_GRACE years at the edition is judged in the
# year it reaches that age, or the last year of the Index counts when that comes first.
WELL_KNOWN_TOP = 0.10
WELL_KNOWN_GRACE = 6
WELL_KNOWN_AGE = 10          # the age at which --stage citations reports when works crossed
WELL_KNOWN_MIN_WORKS = 1000
SCAN_PROCESSES = 8           # parallel readers of the OpenCitations Index

# An edition counts as off-power only if its parsed committee is plausibly complete: at
# least MIN_PC_SIZE pc + senior_pc rows, and at least MIN_PC_SHARE of the median of the
# stream's editions within PC_SIZE_WINDOW years.
MIN_PC_SIZE = 10
MIN_PC_SHARE = 0.5
PC_SIZE_WINDOW = 2

GATE_REFERENCES = 10         # Westerbaan: a seat with more references than this is an outlier

# The member's dblp publications before the year of the edition, of these record types
# (not Informal, Reference, Data, Editorship or Withdrawn).
PUB_TYPES = ["Article", "Inproceedings", "Incollection", "Book"]

# A citation counts only when its paper is checkable and no author of the paper explains it.
# Checkable: an author has at least MIN_AUTHOR_PAPERS other publications (PUB_TYPES) with
# references in the OpenCitations Index. Explained: an author cites the work in another of
# their publications outside the editions where the member holds a role, or co-authored with
# the member in or before the edition's year. The papers of a seat and of its rates are the
# checkable ones. False counts every citation and paper.
CHECK_CITING_AUTHORS = True
MIN_AUTHOR_PAPERS = 10
INSERT_BATCH = 1000          # rows per INSERT statement

# ---------------------------------------------------------------------------
# The model (bbn_infer.py)
# ---------------------------------------------------------------------------

# Bins of the surplus, cites - expected: SURPLUS_EDGES are the lower edges, in citations, of
# every bin after the first.
STATES = ["typical", "mild", "high", "extreme"]
SURPLUS_EDGES = [5, 10, 20]
# depth: citations per citing paper over the expected citations per citing paper. A seat in a
# bin after typical with depth at least DEPTH_EDGE is deep.
DEPTH_EDGE = 1.5
# prom: bands of the expected citations; lower edges of every band after the first.
PROM_BANDS = [0.5, 2, 5, 15, 50]
# A seat with the peer rate has bands of the member's publications instead.
PUBS_BANDS = [15, 60, 180]

MIN_SEATS = 5                # a member is scored with at least this many seats

PRIOR = 0.95                 # P(genuine)
ALPHA = 0.50                 # share of a non-genuine member's seats with coerced citations

# The bin distribution of a coerced seat. Keys match STATES; sums to 1.
MANIP_DIST = {"typical": 0.0, "mild": 0.20, "high": 0.40, "extreme": 0.40}
MANIP_DEEP = 0.70            # share of a coerced seat's bin that is deep

PSEUDO_COUNT = 0.5           # added to every bin of every baseline distribution
FALLBACK_MIN_N = 30          # seats a baseline level needs after leaving out the member's

SHORTLIST_THRESHOLD = 0.50   # P(genuine) below this is shortlisted

WATCH_PIDS = ["n/GonzaloNavarro"]   # always printed with their rank: Westerbaan's SPIRE member

# ---------------------------------------------------------------------------
# The injection test (bbn_inject.py)
# ---------------------------------------------------------------------------

# A coerced seat: the member reviews REVIEWS_PER_PAPER * papers / pc_size papers of the
# edition, rounded up, and each of those cites `extra` more of the member's works.
REVIEWS_PER_PAPER = 3
INJECT_EXTRA = [1, 2, 3, 5]
INJECT_SEATS = [1, 2, 3]     # coerced seats per member
# The planted citations count as unexplained (CHECK_CITING_AUTHORS), or are explained as often
# as the real ones that no co-author explains: cites elsewhere / (cites elsewhere +
# unexplained) in the summary of --stage authors.
INJECT_EXPLAINED = [0.0, 0.54]
INJECT_MEMBERS = 500         # drawn from the scored members that no variant shortlists
INJECT_SEED = 1

# ---------------------------------------------------------------------------
# The sensitivity analysis (bbn_report.py)
# ---------------------------------------------------------------------------
# One setting changed at a time: per group, a label and the rank_corpus arguments that differ
# from the run. Every other setting stays as above.
SENSITIVITY_VARIANTS = {
    "prior": [
        ("pi 0.98", {"prior": 0.98}),
        ("pi 0.99", {"prior": 0.99}),
    ],
    "alpha": [
        ("alpha 0.30", {"alpha": 0.30}),
        ("alpha 0.40", {"alpha": 0.40}),
        ("alpha 0.60", {"alpha": 0.60}),
        ("alpha 0.70", {"alpha": 0.70}),
    ],
    "m(t)": [
        ("towards mild .40/.40/.20",
         {"manip": {"typical": 0.0, "mild": 0.40, "high": 0.40, "extreme": 0.20}}),
        ("towards extreme .10/.30/.60",
         {"manip": {"typical": 0.0, "mild": 0.10, "high": 0.30, "extreme": 0.60}}),
    ],
    "deep share": [
        ("deep share 0.50", {"manip_deep": 0.50}),
        ("deep share 0.90", {"manip_deep": 0.90}),
        ("no depth", {"depth_edge": math.inf, "manip_deep": 0.0}),
    ],
    "surplus edges": [
        ("edges 4/8/16", {"edges": [4, 8, 16]}),
        ("edges 6/12/24", {"edges": [6, 12, 24]}),
    ],
    "depth edge": [
        ("depth edge 1.25", {"depth_edge": 1.25}),
        ("depth edge 2", {"depth_edge": 2.0}),
    ],
    "n_min": [
        ("n_min 3", {"min_seats": 3}),
        ("n_min 10", {"min_seats": 10}),
    ],
}
