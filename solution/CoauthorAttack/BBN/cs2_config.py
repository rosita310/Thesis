"""
Settings of the co-author-editor BBN pipeline, grouped by the script that uses
them. The scripts take only paths on the command line.
"""

from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent

ENV_PATH = BASE_DIR / ".." / ".." / ".env"
DUMP_PATH = BASE_DIR / ".." / ".." / "dblp.nt" / "dblp.nt"

REPORTS_DIR = BASE_DIR / "reports"      # dblp_extract.py output: extract/ + the journal mapping
BBN_DIR = BASE_DIR / "bbn"              # bbn_extract.py output, and everything after it

# The database schemas holding front matter (front_matter_issues + front_matter_board).
PUBLISHERS = ["acm", "ieee"]

# ---------------------------------------------------------------------------
# Corpus construction (bbn_extract.py)
# ---------------------------------------------------------------------------

# Bands of shared papers outside the edited journal; one state per band.
OUTSIDE_BANDS = [1, 4]                   # inclusive upper edges: 1 | 2-4 | 5+
OUTSIDE_STATES = ["outside_1", "outside_2_4", "outside_5plus"]
STATES = ["outside_5plus", "outside_2_4", "outside_1", "repeated", "single"]

REPEATED_MIN = 2             # R_min: shared papers inside the journal for `repeated`
JUNIOR_MAX_PUBS = 10         # career stage: `junior` up to this many publications

FIT_STATES = ["peripheral", "typical", "core"]
FIT_MIN_COLLABS = 5          # a co-author with fewer collaborators gets no fit
MAX_AUTHORS = 40             # papers with more authors are left out of the networks
NETWORK_BANDS = [20, 60, 150]   # bands of collaborator count; fit compares within a band

# ---------------------------------------------------------------------------
# The model (bbn_infer.py)
# ---------------------------------------------------------------------------

PRIOR = 0.95                 # pi = P(G=1)
ALPHA = 0.50                 # coerced fraction of a non-genuine editor's load

# m(s): the state distribution of a coerced co-authorship. Keys match STATES; sums to 1.
MANIP_DIST = {"outside_5plus": 0.0, "outside_2_4": 0.03, "outside_1": 0.10,
              "repeated": 0.15, "single": 0.72}

# m(f | s): the fit distribution of a coerced co-authorship, per state.
MANIP_FIT = {
    "outside_5plus": {"peripheral": 1 / 3, "typical": 1 / 3, "core": 1 / 3},
    "outside_2_4":   {"peripheral": 0.45, "typical": 0.35, "core": 0.20},
    "outside_1":     {"peripheral": 0.50, "typical": 0.35, "core": 0.15},
    "repeated":      {"peripheral": 0.25, "typical": 0.40, "core": 0.35},
    "single":        {"peripheral": 0.60, "typical": 0.30, "core": 0.10},
}

PSEUDO_COUNT = 0.5           # lambda: added to every bin of every baseline distribution
FALLBACK_MIN_N = 30.0        # N_min: post-exclusion load a fallback level needs

CONDITION_ON_CAREER_STAGE = False   # also condition the genuine baseline on career stage
INOUT_GATE = True            # rank only editors who pass Westerbaan's in/out gate (fig. 8.7)
SHORTLIST_THRESHOLD = 0.50   # P(genuine) below this is shortlisted

# ---------------------------------------------------------------------------
# The report (bbn_report.py)
# ---------------------------------------------------------------------------

ALPHA_SWEEP = [0.30, 0.40, 0.50, 0.60, 0.70]
PRIOR_SWEEP = [0.99, 0.98, 0.95, 0.90, 0.80]

# Alternative shapes for m(s).
MANIP_SWEEP = {
    "baseline":  dict(MANIP_DIST),
    "hard-out":  {"outside_5plus": 0.0, "outside_2_4": 0.0, "outside_1": 0.0,
                  "repeated": 0.15, "single": 0.85},
    "one-off":   {"outside_5plus": 0.0, "outside_2_4": 0.02, "outside_1": 0.08,
                  "repeated": 0.05, "single": 0.85},
    "captive":   {"outside_5plus": 0.0, "outside_2_4": 0.05, "outside_1": 0.15,
                  "repeated": 0.40, "single": 0.40},
    "soft-out":  {"outside_5plus": 0.0, "outside_2_4": 0.10, "outside_1": 0.20,
                  "repeated": 0.15, "single": 0.55},
    "eps-out":   {"outside_5plus": 0.05, "outside_2_4": 0.03, "outside_1": 0.10,
                  "repeated": 0.15, "single": 0.67},
}

REPEATED_SWEEP = [2, 3, 4]
OUTSIDE_READINGS = ["any", "strict"]   # strict: a venue the editor also edits is not outside

MIN_CONTROL_NODES = 3        # pre-tenure co-authorships needed for a per-editor comparison

# ---------------------------------------------------------------------------
# The placebo (bbn_placebo.py)
# ---------------------------------------------------------------------------

PLACEBO_SAMPLES = 50_000     # Monte-Carlo draws per editor
PLACEBO_SEED = 3             # each editor's draws are seeded from this and their id
PLACEBO_RECORD_BANDS = [2, 10, 50, 200]   # publication-count bands for matched stand-ins
