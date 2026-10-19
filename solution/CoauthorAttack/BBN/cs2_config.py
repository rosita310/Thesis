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

MAX_AUTHORS = 40             # papers with more authors are left out of the networks

# ---------------------------------------------------------------------------
# The model (bbn_infer.py)
# ---------------------------------------------------------------------------

PRIOR = 0.95                 # pi = P(G=1)
ALPHA = 0.50                 # coerced fraction of a non-genuine editor's load

# m(s): the state distribution of a coerced co-authorship. Keys match STATES; sums to 1.
MANIP_DIST = {"outside_5plus": 0.0, "outside_2_4": 0.03, "outside_1": 0.10,
              "repeated": 0.15, "single": 0.72}

PSEUDO_COUNT = 0.5           # lambda: added to every bin of every baseline distribution
FALLBACK_MIN_N = 30.0        # N_min: post-exclusion load a fallback level needs; also the
                             # collaborators a co-author needs for the personal adjustment

# The personal one-off baseline. For a co-author with at least FALLBACK_MIN_N
# collaborators, g(single) is the journal's times the habit ratio, the co-author's one-off
# habit over the journal's typical habit, at most this cap; the other states are rescaled
# to make up the rest.
PERSONAL_SINGLE_CAP = 0.95

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

# The boundary of the personal adjustment (collaborators the co-author needs), swept for
# the adjustment only; the model itself uses FALLBACK_MIN_N.
POOL_MIN_SWEEP = [10, 30, 50]
# Bins of the habit ratio (the co-author's one-off habit over the journal's typical habit)
# for the calibration table.
HABIT_RATIO_BINS = [0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 2.0]   # upper edges, exclusive

MIN_CONTROL_NODES = 3        # pre-board co-authorships needed for a per-editor comparison

# Entry conditions: rank only editors with at least this many suspect papers (n_min).
MIN_RECORD_SWEEP = [1, 2, 3, 5]

# The head of the ranking: editors counted below these P(genuine), and below PRIOR.
HEAD_P_LEVELS = [0.50, 0.90]

# ---------------------------------------------------------------------------
# The placebo (bbn_placebo.py)
# ---------------------------------------------------------------------------

PLACEBO_SAMPLES = 50_000     # Monte-Carlo draws per editor
PLACEBO_SEED = 3             # each editor's draws are seeded from this and their id
PLACEBO_RECORD_BANDS = [2, 10, 50, 200]   # publication-count bands for matched stand-ins
PATTERN_MIN_COLLABS = 5      # a partner with fewer collaborators has no pattern to rank into
PATTERN_NETWORK_BANDS = [20, 60, 150]   # collaborator-count bands for similar-size stand-ins

# ---------------------------------------------------------------------------
# The injection test (bbn_inject.py)
# ---------------------------------------------------------------------------

# Fake editors who built a career on coercion, scored one at a time without the gate.
INJECT_RECORDS = [15, 30]            # papers in the journal they edit
INJECT_SHARES = [1 / 3, 1 / 2, 2 / 3]   # the coerced share of those papers
INJECT_COAUTHORS = 3         # co-authors per coerced paper: the median of the suspect papers
# one-off:   every coerced paper has new co-authors, so each co-authorship is `single`
# standing:  the same co-authors on every coerced paper, so `repeated` from R_min papers on
# both:      half of the coerced papers one-off, half standing
INJECT_PROFILES = ["one-off", "standing", "both"]
INJECT_BOARDS = [1, 2]       # boards the one-off profile is spread over
INJECT_DRAWS = 20            # fake editors per journal and kind
INJECT_SEED = 7
