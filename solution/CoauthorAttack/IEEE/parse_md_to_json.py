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

MAX_FILES = 15000                       # Set to None to process all valid files
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
    "editor-in-chief.": "Editor-in-Chief",
    "editors-in-chief": "Co-Editor-in-Chief",
    "editors in chief": "Co-Editor-in-Chief",
    "co-editor-in-chief": "Co-Editor-in-Chief",
    "co-editors-in-chief": "Co-Editor-in-Chief",
    "guest editor-in-chief": "Guest Editor-in-Chief",
    "guest editors-in-chief": "Guest Editor-in-Chief",
    "guest editor in chief": "Guest Editor-in-Chief",
    "guest editors in chief": "Guest Editor-in-Chief",
    "deputy editor-in-chief": "Deputy Editor-in-Chief",
    "deputy editor in chief": "Deputy Editor-in-Chief",
    "deputy editors-in-chief": "Deputy Editor-in-Chief",
    "deputy editors in chief": "Deputy Editor-in-Chief",
    "web editor-in-chief": "Web Editor-in-Chief",
    "ieee sensors editor-in-chief": "IEEE Sensors Editor-in-Chief",
    "assistant to the editor-in-chief": "Assistant to the Editor-in-Chief",
    "assistant to the editors-in-chief": "Assistant to the Editor-in-Chief",
    "assistant to the editor in chief": "Assistant to the Editor-in-Chief",
    "assistant to the editors in chief": "Assistant to the Editor-in-Chief",
    "associate editor": "Associate Editor",
    "associate editors": "Associate Editor",
    "specialized associate editor": "Associate Editor",
    "associate editor for portuguese": "Associate Editor",
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
    "senior area editor": "Senior Area Editor",
    "transasctions executive editor": "Transactions Executive Editor",
    "topical editor": "Topical Editor",
    "topical editor-at-large": "Topical Editor-at-Large",
    "associate editor, abstract translations": "Associate Editor",
    "associate editor, history": "Associate Editor",
    "associate editor, tutorials": "Associate Editor",
    "associate editor, case studies": "Associate Editor",
    "associate editor, teaching cases": "Associate Editor",
    "associate editor, research articles": "Associate Editor",
    "associate editor at large": "Associate Editor",
    "assoc. editor, comput. eng.": "Associate Editor",
    "assoc. editor, comput.": "Associate Editor",
    "assoc. editor, commun. syst.": "Associate Editor",
    "board of editors": "Board of Editors",
    "board of associate editors": "Board of Associate Editors",
    "chief editor": "Chief Editor",
    "co-editor": "Co-Editor",
    "co-editor -": "Co-Editor",
    "contributing editor": "Contributing Editor",
    "contributing editor, history": "Contributing Editor",
    "components and systems senior editor": "Components and Systems Senior Editor",
    "corresponding editor": "Corresponding Editor",
    "deputy editor in chief": "Deputy Editor-in-Chief",
    "e-newsletter editor": "E-Newsletter Editor",
    "executive editorial committee": "Executive Editorial Committee",
    "executive editorial board": "Executive Editorial Board",
    "editor-in-chief of ieee cg&a": "Editor-in-Chief of IEEE CG&A",
    "editor-in-chief of ieee tvcg": "Editor-in-Chief of IEEE TVCG",
    "editor-in-chief,": "Editor-in-Chief",
    "editor in-chief": "Editor-in-Chief",
    "editor-in-chief at large": "Editor-in-Chief at Large",
    "editor-in-chief, npss": "Editor-in-Chief, Npss",
    "editor)": "Editor",
    "editor, history": "Editor",
    "editor, transactions/journals": "Editor",
    "ieee magnetics society newsletter editor": "IEEE Magnetics Society Newsletter Editor",
    "jssc editor": "JSSC Editor",
    "j-stsp senior editorial board": "J-STSP Senior Editorial Board",
    "lead series editor": "Lead Series Editor",
    "magazine editor-in-chief": "Magazine Editor-in-Chief",
    "mic senior editor": "MIC Senior Editor",
    "newsletter editor": "Newsletter Editor",
    "nuclear and space radiation effects senior editor": "Nuclear and Space Radiation Effects Senior Editor",
    "nuclear medical and imaging sciences senior editor": "Nuclear Medical and Imaging Sciences Senior Editor",
    "nss conference editor": "NSS Conference Editor"    ,
    "power engineering letters editor-in-chief": "Power Engineering Letters Editor-in-Chief",
    "mic conference editor": "MIC Conference Editor",
    "rtc conference editor": "RTC Conference Editor",
    "publications editor": "Publications Editor",
    "publicity editor": "Publicity Editor",
    "radiation instrumentation senior editor": "Radiation Instrumentation Senior Editor",
    "real time computing senior editor": "Real Time Computing Senior Editor",
    "rtc senior editor": "RTC Senior Editor",
    "senior editor associate": "Senior Editor Associate",
    "senior publications editor": "Senior Publications Editor",
    "senior managing editor": "Senior Managing Editor",
    "senior managing editor,": "Senior Managing Editor,",
    "special issue senior editor": "Special Issue Senior Editor",
    "scint 2007 senior editor": "Scint 2007 Senior Editor",
    "sorma 2008 senior editor": "Sorma 2008 Senior Editor",
    "tbd awards & lecturers committee": "TBD Awards & Lecturers Committee",
    "t-ip editorial board": "T-IP Editorial Board",
    "tcc editor-in-chief": "TCC Editor-in-Chief",
    "t&s magazine editorial board": "T&S Magazine Editorial Board",
    "transactions editor": "Transactions Editor",
    "transactions editor-in-chief": "Transactions Editor-in-Chief",
    "transactions editorial board": "Transactions Editorial Board",
    "transactions executive editor": "Transactions Executive Editor",
    "ieee editorial board": "IEEE Editorial Board",
    "ieee transactions on smart grid editor-in-chief": "IEEE Transactions on Smart Grid Editor-in-Chief",
    "ieee sensors journal editor-in-chief": "IEEE Sensors Journal Editor-in-Chief",
    "ieee journal of quantum electronics editor": "IEEE Journal of Quantum Electronics Editor",
    "editorial board of ieee transactions on circuits and systems—i": "Editorial Board of IEEE Transactions on Circuits and Systems—I",
    "power engineering letters editor-in-chief": "Power Engineering Letters Editor-in-Chief",
    "business editor": "Business Editor",
    "associate editor for the far east": "Associate Editor",
    "associate editor for europe and africa": "Associate Editor",
    "editor for asia": "Editor for Asia",
    "editor for europe": "Editor for Europe",
    "transactions editor-in-chief, tsmc": "Transactions Editor-in-Chief",
	"transactions editor-in-chief, tcyb": "Transactions Editor-in-Chief",
	"transactions editor-in-chief, thms": "Transactions Editor-in-Chief",
	"transactions editor-in-chief, tcss": "Transactions Editor-in-Chief",
    "transactions editor in chair": "Transactions Editor in Chair",
    "associate editor; editorial services": "Associate Editor; Editorial Services",
    "editor-in-chief, systems": "Editor-in-Chief, Systems",
    "editor-in-chief, trabsactions": "Editor-in-Chief, Transactions",
    "antennas & wireless propagation letters editor-in-chief": "Editor-in-Chief",
	"digital archive editor-in-chief": "Editor-in-Chief",
	"electronic publications editor-in-chief": "Editor-in-Chief",
    "clinical informatics - section editor": "Section Editor",
    "bioinformatics - section editor": "Section Editor",
    "imaging informatics - section editor": "Section Editor",
    "public health informatics - section editor": "Section Editor",
	"ieee/osa journal of lightwave technology editor": "Editor",
	"ieee photonics technology letters editor": "Editor",
	"ieee journal of quantum electronics editor": "Editor",
	"ieee journal of selected topics in quantum electronics editor": "Editor",
	"photonics society portal editor": "Editor",
	"leos portal editor": "Editor",
	"ieee /osa journal of lightwave technology editor": "Editor",
	"ieee photonics technology letters editor": "Editor",
}

IGNORED_ROLES = {
    "advisory committee",
    "and products boards",
    "autotestcon board of directors",
    "awards chair",
    "awards and recognition committee",
    "birds of a feather chair",
    "board of meetings",
    "board",
    "board member and eda consultant",
    "canada excellence research chair",
    "chair",
    "chair,",
    "chair, best paper award committee",
    "chair, international council",
    "chair, meetings council",
    "chair, member and education services council",
    "chair, membership engagement and development council",
    "chair, osa industry development associates",
    "chair, publications council",
    "chapter chair liason",
    "chief executive officer",
    "chief financial officer",
    "chief information digital officer",
    "chief information and digital officer",
    "chief marketing officer",
    "chief governance officer",
    "chief information officer",
    "chief human resources officer",
    "chief publication officer",
    "committee chair",
    "conference committee chair",
    "corporate relations director",
    "coordinator",
    "deputy chair",
    "director of journals",
    "eda electron. board syst., anal. verification",
    "editor-in-chief emeritus",
    "education director",
    "ethics officer",
    "finance chair",
    "functional committee chairmen",
    "managing director",
    "editorial director",
    "general chair",
    "globalization director",
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
    "past editor in chief",
    "director, division i",
    "director, division ii",
    "director, division iii",
    "director, division iv",
    "director, division v",
    "director, division vi",
    "director, division vii",
    "division vii director",
    "director-elect, division vii",
    "director & delegate, division vii",
    "director, division x",
    "director, editorial services:",
    "director, production services:",
    "associate director, editorial services:",
    "director, editorial services",
    "director, production services",
    "director & chief operating officer",
    "director, book and information services",
    "director, ieee-standards association",
    "director, journals and magazines",
    "director of production and portfolio management"
    "associate director, editorial services",
    "technical committee chairs",
    "compliance officer",
    "board of governors",
    "committee chairpersons and representatives",
    "director & delegate, division i",
    "executive committee",
    "steering committee",
    "committe chair",
    "academic affairs chair",
    "eds operations director",
    "j-pv steering committee",
    "director of research",
    "technical committee chairs",
    "publications board chair",
    "founding chair",
    "nominations and appointments chair",
    "website coordinator",
    "chair of power engineering",
    "professor and grainger chair",
    "director regional",
    "director regional pasado",
    "director regional electo",
    "ieee/asme transactions on mechatronics management committee",
    "ieee/optica publishing group journal coordinating committee",
    "ieee/osa journal coordinating committee",
    "i2mtc board of directors",
    "journal coordinator",
    "manging director, standards",
    "management committee chairperson",
    "management committee members",
    "medical and imaging sciences (nmis) technical committee.",
    "online communities chair",
    "past-chair",
    "pes publications board chair",
    "production coordinator",
    "publications chair",
    "publications and pspb chair",
    "publications committee chair",
    "publications chair (aipp)",
    "publications chair (ieee)",
    "publicity chair",
    "pspb past chair",
    "pspb vice-chair",
    "services and products board",
    "standing committee chairs",
    "signal processing technical committee",
    "evaluation committee chair and vice chair",
    "sc chair",
    "vice-chair",
    "vice chair-elect",
    "vis international program committee",
    "infovis international program committee",
    "the ieee visualization and graphics technical committee",
    "technical committee chairmen",
    "technical committee",
    "ieee visualization and graphics technical committee (vgtc)",
    "ieee information visualization conference general chair",
    "vgtc chair",
    "director — conf. dev.",
    "director — conf. operations",
    "journals board",
    "director—ap region",
    "director—conf. dev.",
    "director—conf. operations",
    "director—educational services",
    "magazines board",
    "director of magazines",
    "director—emea region",
    "director—journals",
    "director—la region",
    "director—magazines",
    "director—tech. committees",
    "director—industry communities",
    "director—industry outreach",
    "director—member services",
    "director—na region",
    "director—on-line content",
    "director—standards dev.",
    "director–standarization programs dev.",
    "committee on earth observation",
    "ieee conferences committee",
    "ieee-usa energy policy committee",
    "ieee-usa r & d policy committee",
    "ieee–usa technology policy committee",
    "ieee aess publications board",
    "gold representative",
    "director; editorial services",
    "director; production services",
    "standing committee chairpersons",
    "director",
    "art director",
    "senior art director",
    "director, ieee division vi",
    "asian representative",
    "young professionals program chair",
    "women in sensors committee chair",
    "research chair in future wireless technologies",
    "conference chair",
    "distinguished lecturer chair",
    "education chair",
    "young professionals chair",
    "marketing chair",
    "membership development chair",
    "nominations chair",
    "fellows chair",
    "strategic planning chair",
    "ub distinguished professor and chair",
    "transnational committee",
    "ibc representative",
    "ap-s/ursi joint meetings committee",
    "awards coordinator",
    "committee on man and radiation",
    "standards committee chair",
    "meetings committee chair",
    "awards & lecturers committee",
    "finance committee",
    "ieee technology policy council chair",
    "committee on transportation & technology policy",
    "committee on communications & information policy",
    "leos fellows evaluation committee",
    "photonics society fellows evaluation committee",
}

TERMINATING_ROLES = {
    "ieee periodicals magazines department",
    "ieee staff",
}

TERMINATING_TEXTS = [
    "ieee prohibits discrimination",
    "copyright and reprint permissions",
]

SUBJECT_AREAS = {
    "coding", "communication systems i", "communication systems ii",
    "wireless communications i", "wireless communications ii",
    "wireless networks i", "wireless networks ii", "communication theory",
    "network architecture", "machine learning", "resource management and optimization",
    "signal processing i", "signal processing ii"
}

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

def is_valid_inline_role(role_str: str) -> bool:
    """Helper to check if a string looks like a role."""
    role_str = role_str.lower()
    if role_str in ROLE_MAPPING:
        return True
    return any(re.search(rf"\b{kw}\b", role_str) for kw in ROLE_KEYWORDS)

def is_valid_name(name_str: str) -> bool:
    """Returns False if the string contains any numeric digits. Can be expanded with more heuristics."""
    return not any(char.isdigit() for char in name_str)

def parse_inline_entry(line: str) -> tuple[str, str] | None:
    """Checks if a line contains 'Name, Role' or 'Role: Name' inline format."""
    # Length limit for inline entries to avoid capturing heavily punctuated prose
    if len(line) > 100:
        return None

    # Pattern 0: 'Name (Role)'
    paren_match = re.search(r"^(.*?)\s*\((.*?)\)$", line)
    if paren_match:
        potential_name = paren_match.group(1).strip()
        potential_role = paren_match.group(2).strip().lower()
        if is_valid_inline_role(potential_role):
            if not is_valid_name(potential_name):
                return None
            mapped_role = ROLE_MAPPING.get(potential_role, f"Unmapped: {potential_role}")
            return potential_name, mapped_role

    # Pattern 1: 'Role: Name'
    if ":" in line:
        parts = line.split(":", 1)
        potential_role = parts[0].strip().lower()
        potential_name = parts[1].strip().rstrip(',') # Clean trailing commas on names
        if is_valid_inline_role(potential_role):
            if not is_valid_name(potential_name):
                return None
            
            mapped_role = ROLE_MAPPING.get(potential_role, f"Unmapped: {potential_role}")
            return potential_name, mapped_role

    # Pattern 2: 'Name, Role' (Using rsplit to handle names that have commas like Jr., Ph.D.)
    if "," in line:
        parts = line.rsplit(",", 1)
        potential_name = parts[0].strip()
        potential_role = parts[1].strip().lower().rstrip(',') # Clean trailing commas on roles
        if is_valid_inline_role(potential_role):
            if not is_valid_name(potential_name):
                return None
            mapped_role = ROLE_MAPPING.get(potential_role, f"Unmapped: {potential_role}")
            return potential_name, mapped_role

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

        # Skip subject area sub-headers
        if clean_line in SUBJECT_AREAS:
            continue

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
            if role.startswith("Unmapped:"):
                log_unmapped_role(role, journal_name)
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
            if not is_valid_name(line):
                continue  # Skip lines with numbers; they aren't names
            current_name = line
            continue

        # Heuristic to separate consecutive editors without emails
        if current_name and not ("@" in line):
            # If the line is short, has no commas, no affiliation keywords, AND no numbers
            if (len(line) < 30 
                and "," not in line 
                and not any(inst in clean_line for inst in INSTITUTION_KEYWORDS)
                and is_valid_name(line)):
                extracted_editors.append({
                    "name": current_name,
                    "role": current_role,
                    "association": " ".join(affiliation_lines).strip(),
                })
                current_name = line
                affiliation_lines = []
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