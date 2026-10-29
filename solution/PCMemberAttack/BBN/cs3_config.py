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
