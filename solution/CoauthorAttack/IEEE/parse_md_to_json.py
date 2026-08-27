from __future__ import annotations
import json
import logging
from pathlib import Path
import re
import unicodedata

# ==========================================
# CONFIGURATION
# ==========================================

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
LOG_FILE = BASE_DIR / "parsing.log"

MAX_FILES = 500                         # Set to None to process all valid files
OVERWRITE_EXISTING = True               # True: re-parse and overwrite. False: skip if JSON exists.
SKIP_PREFIXES = (                       # Files starting with these prefixes will be skipped
    
)

# ==========================================
# HEURISTIC CONFIGURATION
# ==========================================
UNMAPPED_ROLES_FILE = BASE_DIR / "unmapped_roles.txt"

ROLE_KEYWORDS = {
    "editor", "chair", "committee", "president", "director", 
    "board", "representative", "officer", "vice", "coordinator"
}

INSTITUTION_KEYWORDS = {
    "university", "univ", "institute", "inst", "department", "dept", 
    "inc", "corp", "laboratory", "lab", "center", "college", "school"
}

BLOCKLIST_KEYWORDS = {
    "cf.", "silicon device", "technology", "editorial:"
}

# ==========================================
# PARSER CONFIGURATION
# ==========================================

ROLE_MAPPING = {
    "editor-in-chief": "Editor-in-Chief",
    "editor in chief": "Editor-in-Chief",
    "editors-in-chief": "Co-Editor-in-Chief",
    "editors in chief": "Co-Editor-in-Chief",
    "co-editor-in-chief": "Co-Editor-in-Chief",
    "co-editors-in-chief": "Co-Editor-in-Chief",
    "guest editor-in-chief": "Guest Editor-in-Chief",
    "guest editors-in-chief": "Guest Editor-in-Chief",
    "guest editor in chief": "Guest Editor-in-Chief",
    "guest editors in chief": "Guest Editor-in-Chief",
    "assistant to the editor-in-chief": "Assistant to the Editor-in-Chief",
    "assistant to the editors-in-chief": "Assistant to the Editor-in-Chief",
    "assistant to the editor in chief": "Assistant to the Editor-in-Chief",
    "assistant to the editors in chief": "Assistant to the Editor-in-Chief",
    "associate editor": "Associate Editor",
    "associate editors": "Associate Editor",
    "specialized associate editor": "Associate Editor",
    "senior associate editor": "Senior Associate Editor",
    "senior associate editors": "Senior Associate Editor",
    "senior associate editor and acting editor-in-chief": "Senior Associate Editor",
    "sr. associate editor": "Senior Associate Editor",
    "sr. associate editors": "Senior Associate Editor",
    "editorial board": "Editorial Board Member",
    "editorial board member": "Editorial Board Member",
    "editorial board members": "Editorial Board Member",
    "editorial assistant": "Editorial Assistant",
    "area editors": "Area Editor",
    "area editors, ai, ml and data science for sustainable societies": "Area Editor",
    "area editors, development, economics and policy": "Area Editor",
    "area editors, environment, sustainability and climate change": "Area Editor",
    "area editors, hci, design and critical perspectives": "Area Editor",
    "area editors, systems and iot for sustainable societies": "Area Editor",
    "area editors, technology, media, and social practice": "Area Editor",
    "algorithms editor": "Algorithms Editor",
    "on-line editor": "Online Editor",
    "online editor": "Online Editor",
    "special issue editor": "Special Issue Editor",
    "special issue editors": "Special Issue Editor",
    "special issue associate editors and advisors": "Special Issue Editor",
    "outreach editor": "Outreach Editor",
    "outreach editors": "Outreach Editor",
    "distinguished reviewer board": "Distinguished Reviewer Board Member",
    "distinguished reviewer": "Distinguished Reviewer",
    "managing editor": "Managing Editor",
    "managing editors": "Managing Editor",
    "perspective articles editor": "Perspective Articles Editor",
    "survey and tutorial papers editor": "Survey and Tutorial Papers Editor",
    "release papers editor": "Release Papers Editor",
    "senior advisory editor": "Senior Advisory Editor",
    "senior advisory editors": "Senior Advisory Editor",
    "social media editor": "Social Media Editor",
    "review board": "Review Board Member",
    "north america editor": "North America Editor",
    "book review editor": "Book Review Editor",
    "guest editor": "Guest Editor",
    "guest editors": "Guest Editor",
    "assistant editor": "Assistant Editor",
    "assistant editors": "Assistant Editor",
    "editor": "Editor",
    "editors": "Editor",
    "reprodicibility board": "Reproducibility Board Member",
    "reproducibility editorial board": "Reproducibility Board Member",
    "associate editors for feature articles": "Associate Editor",
    "ieee antennas and propagation magazine editorial board": "Editorial Board Member",
    "track editor": "Track Editor",
    "senior editor": "Senior Editor",
    "executive editor": "Executive Editor",
    "outgoing editor-in-chief": "Outgoing Editor-in-Chief",
    "associate editor-in-chief": "Associate Editor-in-Chief",
    "senior editorial assistant": "Senior Editorial Assistant",
    "senior managing editor": "Senior Managing Editor",
}

IGNORED_ROLES = {
    "chief financial officer",
    "chief marketing officer",
    "chief governance officer",
    "chief information officer",
    "chief human resources officer",
    "chief publication officer",
    "director of journals",
    "managing director",
    "editorial director",
    "production director",
    "journals coordinator",
    "general counsel and chief compliance officer",
    "administrative committee",
    "transactions operations committee",
    "advisory board",
    "advisory board members",
    "information director",
    "information directors",
    "information officer and administrator",
    "assistant information director",
    "administrator",
    "editorial advisor",
    "founding editor",
    "founding co-editors-in-chief",
    "publicity chairs",
    "ieee officers",
    "ieee executive staff",
    "ieee publishing operations",
    "ieee periodicals",
    "president",
    "president-elect",
    "past president",
    "secretary",
    "treasurer",
    "director & secretary",
    "director & treasurer",
    "vice president",
    "staff director",
    "senior director",
    "executive director",
    "associate director",
    "production editor",
    "publications coordinator",
    "manager of administrative services",
    "transactions operations chair",
    "past editors-in-chief",
    "past editor-in-chief",
    "director, division vi",
}

TERMINATING_ROLES = {
    "ieee periodicals magazines department",
    "ieee staff",
}

TERMINATING_TEXTS = [
    "ieee prohibits discrimination",
    "digital object identifier",
    "general information for contributors",
    "copyright and reprint permissions",
]

# ==========================================
# LOGGING SETUP
# ==========================================
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
logging.getLogger().addHandler(console_handler)

# ==========================================
# PARSING LOGIC 
# ==========================================

def is_potential_role_header(line: str) -> bool:
    """Evaluates if a line is likely a custom role or section header based on heuristics."""
    # Length limit: Skip long article titles masquerading as roles
    if len(line) > 75: 
        return False
        
    clean_line = line.lower().strip()
    
    # Exclude all-caps generic plural section headers (e.g., SENIOR EDITORS)
    if line.isupper() and clean_line.endswith('s') and clean_line not in ROLE_MAPPING:
        return False

    # Blocklist check for academic topics and edge-case text
    if any(block_word in clean_line for block_word in BLOCKLIST_KEYWORDS):
        return False

    words = clean_line.split()
    
    if len(words) > 8 or len(words) == 0:
        return False
    if any(inst in clean_line for inst in INSTITUTION_KEYWORDS):
        return False
    # Enforce word boundaries so "vice" doesn't match words like "device" or "service"
    if any(re.search(rf"\b{role_word}\b", clean_line) for role_word in ROLE_KEYWORDS):
        return True
    return False

def log_unmapped_role(role_name: str, journal_name: str):
    """Appends unmapped roles to a file for manual review later."""
    with open(UNMAPPED_ROLES_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{journal_name}] - {role_name}\n")

def parse_inline_entry(line: str) -> tuple[str, str] | None:
    """Checks if a line contains 'Name, Role' or 'Role: Name' inline format."""
    # Length limit for inline entries to avoid capturing heavily punctuated prose
    if len(line) > 100:
        return None
        
    # Pattern 1: 'Role: Name'
    if ":" in line:
        parts = line.split(":", 1)
        potential_role = parts[0].strip().lower()
        potential_name = parts[1].strip().rstrip(',') # Clean trailing commas on names
        if potential_role in ROLE_MAPPING:
            return potential_name, ROLE_MAPPING[potential_role]

    # Pattern 2: 'Name, Role' (Using rsplit to handle names that have commas like Jr., Ph.D.)
    if "," in line:
        parts = line.rsplit(",", 1)
        potential_name = parts[0].strip()
        potential_role = parts[1].strip().lower().rstrip(',') # Clean trailing commas on roles
        if potential_role in ROLE_MAPPING:
            return potential_name, ROLE_MAPPING[potential_role]

    return None

def parse_markdown_file(md_path: Path) -> dict:
    with open(md_path, "r", encoding="utf-8") as file:
        text = file.read()

    text = unicodedata.normalize("NFKC", text)

    # Clean raw lines: remove blank lines, trim spaces, and collapse multiple spaces into one
    raw_lines = [
        re.sub(r"\s+", " ", line).strip()
        for line in text.split("\n")
        if line.strip()
    ]

    # Filter out Table of Contents lines (starts with numbers followed by text/tabs)
    # Also filter out Index Metadata lines (contains a semicolon followed by typical volume/page structures)
    filtered_lines = []
    for line in raw_lines:
        if re.match(r"^\d+[\s\t]+", line):
            continue
        if ";" in line and re.search(r'\d+-\d+', line): # Metadata filter e.g., 'COML Feb 03 52-54'
            continue
        filtered_lines.append(line)

    # Merge Multi-Line Headers (robust against trailing whitespace variations)
    merged_lines = []
    skip_next = False
    for i in range(len(filtered_lines)):
        if skip_next:
            skip_next = False
            continue

        current_line = filtered_lines[i]
        # Check if line ends with prepositions like 'for', 'of', 'and'
        if (
            re.search(r"\b(for|of|and)$", current_line.lower())
            and i + 1 < len(filtered_lines)
        ):
            merged_lines.append(f"{current_line} {filtered_lines[i+1]}")
            skip_next = True
        else:
            merged_lines.append(current_line)

    journal_name = md_path.name
    extracted_editors = []
    current_role = None
    current_name = None
    affiliation_lines = []

    for line in merged_lines:
        clean_line = line.lower()

        # Termination Check
        if clean_line in TERMINATING_ROLES or any(
            t in clean_line for t in TERMINATING_TEXTS
        ):
            if current_name and current_role:
                extracted_editors.append({
                    "name": current_name,
                    "role": current_role,
                    "association": " ".join(affiliation_lines).strip(),
                })
            break

        # Check and Ignore Corporate Officers & Staff (BEFORE inline & header checks)
        if any(role in clean_line for role in IGNORED_ROLES):
            if current_name and current_role:
                extracted_editors.append({
                    "name": current_name,
                    "role": current_role,
                    "association": " ".join(affiliation_lines).strip(),
                })
            current_role = None
            current_name = None
            affiliation_lines = []
            continue

        # Check for Inline Name/Role Combos
        inline_match = parse_inline_entry(line)
        if inline_match:
            if current_name and current_role:
                extracted_editors.append({
                    "name": current_name,
                    "role": current_role,
                    "association": " ".join(affiliation_lines).strip(),
                })
                affiliation_lines = []

            name, role = inline_match
            extracted_editors.append(
                {"name": name, "role": role, "association": ""}
            )
            current_name = None
            current_role = None
            continue

        # Check for Role Headers (Exact Match)
        matched_role = None
        if clean_line in ROLE_MAPPING:
            matched_role = ROLE_MAPPING[clean_line]

        # Fuzzy Heuristic Fallback
        if not matched_role and is_potential_role_header(line):
            matched_role = f"Unmapped: {line}"
            log_unmapped_role(line, journal_name)

        if matched_role:
            if current_name and current_role:
                extracted_editors.append({
                    "name": current_name,
                    "role": current_role,
                    "association": " ".join(affiliation_lines).strip(),
                })
            current_name = None
            affiliation_lines = []
            current_role = matched_role
            continue

        if not current_role:
            continue

        # Parse Name and Affiliations under active roles
        if not current_name:
            current_name = line
            continue

        if "@" in line and "." in line.split("@")[-1]:
            extracted_editors.append({
                "name": current_name,
                "role": current_role,
                "association": " ".join(affiliation_lines).strip(),
            })
            current_name = None
            affiliation_lines = []
        else:
            affiliation_lines.append(line)

    # Flush final editor
    if current_name and current_role:
        extracted_editors.append({
            "name": current_name,
            "role": current_role,
            "association": " ".join(affiliation_lines).strip(),
        })

    return {
        "journal_file": journal_name,
        "editors": extracted_editors,
    }

# ==========================================
# MAIN EXECUTION
# ==========================================

def main():
    if not DATA_DIR.exists():
        logging.error(f"Data directory not found at: {DATA_DIR}")
        return

    logging.info("--- Starting new parsing run ---")
    
    processed_count = 0
    
    for md_path in DATA_DIR.glob("*.md"):
        
        if MAX_FILES is not None and processed_count >= MAX_FILES:
            logging.info(f"Reached maximum file limit ({MAX_FILES}). Stopping.")
            break

        if md_path.name.startswith(SKIP_PREFIXES):
            logging.debug(f"Skipping {md_path.name} (Matches SKIP_PREFIXES).")
            continue
            
        json_path = md_path.with_suffix(".json")
        
        if json_path.exists():
            if OVERWRITE_EXISTING:
                logging.info(f"Overwriting existing JSON for: {md_path.name}")
            else:
                logging.debug(f"Skipping {md_path.name} (JSON already exists).")
                continue

        logging.info(f"Parsing: {md_path.name}")
        try:
            extracted_data = parse_markdown_file(md_path)
            
            with open(json_path, "w", encoding="utf-8") as json_file:
                json.dump(extracted_data, json_file, indent=4, ensure_ascii=False)
                
            logging.info(f"Successfully created: {json_path.name}")
            processed_count += 1
            
        except Exception as e:
            logging.error(f"Failed to parse {md_path.name}. Error: {e}", exc_info=True)

    logging.info(f"Run complete. Successfully processed {processed_count} files.\n")

if __name__ == "__main__":
    main()