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

MAX_FILES = None                        # Set to None to process all valid files
OVERWRITE_EXISTING = False              # True: re-parse and overwrite. False: skip if JSON exists.
SKIP_PREFIXES = (                       # Files starting with these prefixes will be skipped
    "ACM Transactions on Graphics",     # Doesn't contain information we want   
    "Proceedings of the ACM on",        # Divergent structure from other journals. Also, these are conference proceedings, not true journals.
)


# ==========================================
# PARSER CONFIGURATION
# ==========================================

# WANTED ROLES & NORMALIZATION
# Maps variations of a role found in the text to the standard role name we want in the JSON.
# These include variations I did not find but could plausibly exist.
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
}

# IGNORED ROLES
# Roles we don't care about, but finding them shouldn't stop the whole parsing process.
IGNORED_ROLES = {
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
}

# TERMINATING ROLES
# If we see these roles, we know there are no more valid editors after this point.
TERMINATING_ROLES = {
    "acm headquarters staff",
    "administrative support",
    "former editors-in-chief",
    "founding editor-in-chief",
    "headquarters staf",
    "headquarters journals staf",
    "headquarters journals staff",
    "information co-directors",
    "information specialist",
    "journal administrator",
    "past associate editors",
    "past distinguished reviewers",
    "past editors-in-chief",
    "senior advisors",
    "steering committee",
    "traffic manager",
}

# TERMINATING TEXTS
# Non-role lines that indicate the end of the editor section. 
# We will use substring matching for these.
TERMINATING_TEXTS = [
    "ACM European Service Centre",
    "For manuscript submissions and ACM membership information, see inside backcover.",
    "Guide to Manuscript Submission",
    "service, notify your local post office before",
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

# Also print logs to the console for easier debugging
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
logging.getLogger().addHandler(console_handler)

#    Sanitizes markdown text by normalizing unicode, removing control characters,
#    stripping useless lines, and removing standard boilerplate blocks (like the ACM address).
def clean_markdown(text: str) -> str:

    # Normalize Unicode (fixes ligatures like 'ﬁ' -> 'fi' where possible)
    text = unicodedata.normalize("NFKC", text)

    # Remove unwanted ASCII control characters (0x00 - 0x1F)
    text = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", text)

    # Process line by line for empty and junk lines
    lines = text.split('\n')
    valid_lines = []
    
    for line in lines:
        # Condition A: Line MUST contain at least one letter or number
        if not re.search(r'[A-Za-z0-9]', line):
            continue
            
        # Condition B: Remove isolated PyMuPDF ligature artifacts ('f', 'i', 'fi', 'if')
        # We strip all non-alphabetic chars to ignore spaces or formatting like "**f**"
        alpha_only = re.sub(r'[^A-Za-z]', '', line).lower()
        if alpha_only in ['f', 'i', 'fi', 'if']:
            continue
            
        valid_lines.append(line)
        
    # Rejoin the lines (empty lines are now completely gone)
    cleaned_text = '\n'.join(valid_lines)
    
    # Remove the contiguous ACM contact block
    # Using MULTILINE so ^ matches the start of a line.
    acm_block_pattern = re.compile(
        r"^[\*\s_]*ACM[\*\s_]*\n"          # 'ACM' (possibly with markdown bold/italics)
        r"(?:[^\n]+\n){1,2}"               # 1 or 2 generic address lines (I only found cases with 1 line, but just in case)
        r"[^\n]*New York, NY[^\n]*\n"      # The New York state/zip line
        r"[^\n]*Tel\.?[^\n]*\n"            # Telephone line
        r"[^\n]*Fax[^\n]*\n"               # Fax line
        r"[^\n]*acm\.org[^\n]*\n?"         # Primary ACM website line
        r"[^\n]*Home Page:[^\n]*(?:\n|$)", # Home Page line (ignoring any surround formatting)
        re.IGNORECASE | re.MULTILINE
    )
    
    cleaned_text = re.sub(acm_block_pattern, "", cleaned_text)
    
    return cleaned_text

# ==========================================
# PARSING LOGIC 
# ==========================================

def parse_editors_from_blocks(role_blocks: dict) -> list:
    extracted_editors = []
    
    for role, lines in role_blocks.items():
        people_raw = []
        current_person_lines = []
        
        for line in lines:
            clean_start = line.replace("** **", "").strip()
            
            if clean_start.startswith("**"):
                if current_person_lines:
                    first_line = current_person_lines[0].replace("** **", "")
                    first_bold = re.search(r"\*\*(.*?)\*\*", first_line)
                    
                    if first_bold and len(first_bold.group(1).strip().split()) == 1:
                        current_person_lines.append(line)
                    else:
                        people_raw.append(" \n ".join(current_person_lines))
                        current_person_lines = [line]
                else:
                    current_person_lines = [line]
            else:
                if current_person_lines:
                    current_person_lines.append(line)
                    
        if current_person_lines:
            people_raw.append(" \n ".join(current_person_lines))

        eic_count = 0
        for raw_person in people_raw:
            text = raw_person.replace("** **", "")
            text = re.sub(r"\*\*\[(.*?)\](?:\(https?://[^)]+\))?\*\*", r"**\1**", text)
            
            bolds = re.findall(r"\*\*(.*?)\*\*", text)
            if not bolds:
                continue
                
            name = bolds[0].strip()
            if len(name.split()) == 1 and len(bolds) > 1:
                name = name + " " + bolds[1].strip()
                text = text.replace(f"**{bolds[0]}**", "", 1).replace(f"**{bolds[1]}**", "", 1)
            else:
                text = text.replace(f"**{bolds[0]}**", "", 1)

            association = re.sub(r"^[\s_]+|[\s_]+$", "", text.strip())
            association = " ".join(association.split())
            
            assigned_role = role
            if role == "Editor-in-Chief":
                if eic_count > 0:
                    assigned_role = "Associate Editor"
                eic_count += 1
                
            extracted_editors.append({
                "name": name,
                "role": assigned_role,
                "association": association
            })
            
    return extracted_editors

def parse_markdown_file(md_path) -> dict:
    with open(md_path, "r", encoding="utf-8") as file:
        raw_content = file.read()
    
    content = clean_markdown(raw_content)
    lines = content.split('\n')
    
    role_blocks = {}
    current_role = None
    
    for line in lines:
        clean_line = re.sub(r'[*_]', '', line).strip().lower()
        
        if clean_line in TERMINATING_ROLES or any(t in clean_line for t in TERMINATING_TEXTS):
            break
        if clean_line in ROLE_MAPPING:
            current_role = ROLE_MAPPING[clean_line]
            if current_role not in role_blocks:
                role_blocks[current_role] = []
            continue
        if clean_line in IGNORED_ROLES:
            current_role = None
            continue
            
        if current_role:
            role_blocks[current_role].append(line.strip())
            
    extracted_editors = parse_editors_from_blocks(role_blocks)
    
    return {
        "journal_file": md_path.name,
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
    
    # Find all .md files in the data directory
    for md_path in DATA_DIR.glob("*.md"):
        
        # Enforce max file limit
        if MAX_FILES is not None and processed_count >= MAX_FILES:
            logging.info(f"Reached maximum file limit ({MAX_FILES}). Stopping.")
            break

        # Check skip prefixes
        if md_path.name.startswith(SKIP_PREFIXES):
            logging.debug(f"Skipping {md_path.name} (Matches SKIP_PREFIXES).")
            continue
            
        json_path = md_path.with_suffix(".json")
        
        # Check for existing JSON files / Overwrite logic
        if json_path.exists():
            if OVERWRITE_EXISTING:
                logging.info(f"Overwriting existing JSON for: {md_path.name}")
            else:
                logging.debug(f"Skipping {md_path.name} (JSON already exists).")
                continue

        # Parse the file
        logging.info(f"Parsing: {md_path.name}")
        try:
            extracted_data = parse_markdown_file(md_path)
            
            # Write to JSON
            with open(json_path, "w", encoding="utf-8") as json_file:
                json.dump(extracted_data, json_file, indent=4, ensure_ascii=False)
                
            logging.info(f"Successfully created: {json_path.name}")
            processed_count += 1
            
        except Exception as e:
            logging.error(f"Failed to parse {md_path.name}. Error: {e}", exc_info=True)

    logging.info(f"Run complete. Successfully processed {processed_count} files.\n")

if __name__ == "__main__":
    main()