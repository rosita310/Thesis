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
BBN_DIR = BASE_DIR / "bbn"              # bbn_extract.py output, and everything after it

# ---------------------------------------------------------------------------
# Corpus construction (bbn_extract.py)
# ---------------------------------------------------------------------------

SCHEMA = "lncs_citation"     # the materialised citation and paper tables

ROLES = ["pc", "senior_pc"]  # the role categories whose seats are scored

# The off-power rate: the stream's editions in which the member has no role at all, all
# those within RATE_WINDOW years, and further out nearest first while they hold fewer than
# MIN_OFF_PAPERS papers. A stream with too few falls back to the other seats of the edition.
RATE_WINDOW = 2
MIN_OFF_PAPERS = 30
RATE_PSEUDO_COUNT = 0.5      # added to the off-power citations
RATIO_PSEUDO_COUNT = 1       # citation ratio = (x + RATIO_PSEUDO_COUNT) / (E + RATIO_PSEUDO_COUNT)

# An edition counts as off-power only if its parsed committee is plausibly complete: at
# least MIN_PC_SIZE pc + senior_pc rows, and at least MIN_PC_SHARE of the median of the
# stream's editions within PC_SIZE_WINDOW years.
MIN_PC_SIZE = 10
MIN_PC_SHARE = 0.5
PC_SIZE_WINDOW = 2

GATE_REFERENCES = 10         # Westerbaan: a seat with more references than this is an outlier
