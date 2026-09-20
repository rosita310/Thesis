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

MAX_FILES = 4                     
OVERWRITE_EXISTING = True              
SKIP_PREFIXES = ()                      


# ==========================================
# PARSER CONFIGURATION
# ==========================================

WANTED_SECTIONS = [
    "organizing committee",
    "program committee",
    "steering committee",
    "track chairs",
    "organization",
]

IGNORED_SECTIONS = [
    "advisory board",
    "organizer",
    "organizing partners",
    "table of contents",
    "copyright",
    "full papers",
    "work-in-progress",
    "sponsors",
    "author index",
    "message from",
]

NAME_MIN_WORDS = 2
NAME_MAX_WORDS = 5

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
# HELPER FUNCTIONS
# ==========================================

def clean_person_name(name: str) -> str:
    name = name.replace("*", " ").replace("_", " ")
    name = " ".join(name.split()).strip(" ,;:-")
    name = re.sub(r"\.(?=[A-Za-z])", ". ", name)
    return " ".join(name.split())

def is_person_name(name: str) -> bool:
    if not name:
        return False
    if any(character.isdigit() for character in name):
        return False
    if not any(character.isalpha() for character in name):
        return False
    return NAME_MIN_WORDS <= len(name.split()) <= NAME_MAX_WORDS

# ==========================================
# PARSING LOGIC
# ==========================================

def parse_markdown_file(md_path: Path) -> dict:
    with open(md_path, "r", encoding="utf-8") as file:
        raw_content = file.read()
        
    filename = md_path.stem
    isbn_match = re.search(r'_([0-9]+(?:-[0-9]+)+)', filename)
    isbn = isbn_match.group(1) if isbn_match else None
    
    title = filename.rsplit('_', 1)[0] if isbn_match else filename
    title = title.replace('..', '').strip()
        
    text = unicodedata.normalize("NFKC", raw_content)
    lines = text.split('\n')
    
    extracted_people = []
    capture = False
    current_section = "Committee Member"
    current_inline_role = None
    
    for line in lines:
        line_clean = line.strip()
        if not line_clean:
            continue
            
        header_check = re.sub(r'[*#]', '', line_clean).strip().lower()
        is_toc_line = bool(re.search(r'\.{3,}', line_clean))
        
        if not is_toc_line:
            if any(wanted in header_check for wanted in WANTED_SECTIONS) and len(header_check.split()) < 10:
                capture = True
                current_section = header_check.title()
                current_inline_role = None # Reset inline role on new section
                continue
                
            if any(ignored in header_check for ignored in IGNORED_SECTIONS):
                capture = False
                continue
            
        if capture:
            # Format C (Inline Roles with Parentheses): **Role:** Name ( _Association_ )
            match_c = re.match(r'^(?:\*\*(.*?):\*\*)?\s*([A-Za-z\.\s\-]+)\s*\(\s*_*([^)]+?)_*\s*\)$', line_clean)
            if match_c:
                role_inline = match_c.group(1)
                if role_inline:
                    current_inline_role = role_inline.strip().title()
                
                name = clean_person_name(match_c.group(2))
                association = match_c.group(3).strip()
                role = current_inline_role if current_inline_role else current_section
                
                if is_person_name(name):
                    extracted_people.append({
                        "name": name,
                        "role": role,
                        "association": association
                    })
                continue

            # Format A (Bullet points): - **Name**, Association — Role
            match_a = re.match(r'^[\-\*]\s*\*\*(.*?)\*\*(?:,\s*(.*?))?(?:\s*[—\-\u2014]\s*(.*))?$', line_clean)
            if match_a:
                name = clean_person_name(match_a.group(1))
                association = match_a.group(2).strip() if match_a.group(2) else ""
                role = match_a.group(3).strip() if match_a.group(3) else current_section
                
                if is_person_name(name):
                    extracted_people.append({
                        "name": name,
                        "role": role,
                        "association": association
                    })
                continue
                
            # Format B (Plain text comma separation)
            if len(line_clean) < 150 and not line_clean.endswith('.'):
                parts = line_clean.split(',')
                if len(parts) >= 2:
                    name = clean_person_name(parts[0])
                    association = ", ".join([p.strip() for p in parts[1:]])
                    
                    if is_person_name(name):
                        extracted_people.append({
                            "name": name,
                            "role": current_section,
                            "association": association
                        })
                    
    return {
        "conference_title": title,
        "isbn": isbn,
        "conference_file": md_path.name,
        "persons_of_interest": extracted_people,
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