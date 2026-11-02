"""
Settings of the PC member citation BBN pipeline, grouped by the script that uses
them. The scripts take only paths on the command line.
"""

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

SCHEMA = "lncs_citation"     # the materialised citation and paper tables

ROLES = ["pc", "senior_pc"]  # the role categories whose seats are scored

# The off-power rate: the stream's editions in which the member has no role at all, all
# those within RATE_WINDOW years, and further out nearest first, up to MAX_SPAN years, while
# they hold fewer than MIN_OFF_PAPERS papers. Too few falls back to the other seats of the
# edition. E, the expected citations, is n times the larger of that rate and the rate of the
# other seats.
RATE_WINDOW = 2
MAX_SPAN = 5
MIN_OFF_PAPERS = 30
RATE_PSEUDO_COUNT = 0.5      # added to the off-power citations

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

# ---------------------------------------------------------------------------
# The model (bbn_infer.py)
# ---------------------------------------------------------------------------

# Bins of the surplus x - E: SURPLUS_EDGES are the lower edges, in citations, of every bin
# after the first.
STATES = ["typical", "mild", "high", "extreme"]
SURPLUS_EDGES = [10, 20, 40]
# prom: bands of E, the expected citations; lower edges of every band after the first.
PROM_BANDS = [0.5, 2, 5, 15, 50]
# A seat with the peer rate has bands of the member's publications instead.
PUBS_BANDS = [15, 60, 180]

MIN_SEATS = 3                # a member is scored with at least this many seats

PRIOR = 0.95                 # pi = P(G=1)
ALPHA = 0.50                 # share of a non-genuine member's seats with coerced citations

# m(b): the bin distribution of a coerced seat. Keys match STATES; sums to 1.
MANIP_DIST = {"typical": 0.0, "mild": 0.20, "high": 0.40, "extreme": 0.40}

PSEUDO_COUNT = 0.5           # lambda: added to every bin of every baseline distribution
FALLBACK_MIN_N = 30          # N_min: seats a baseline level needs after leaving out the member's

SHORTLIST_THRESHOLD = 0.50   # P(genuine) below this is shortlisted

WATCH_PIDS = ["n/GonzaloNavarro"]   # always printed with their rank: Westerbaan's SPIRE member
