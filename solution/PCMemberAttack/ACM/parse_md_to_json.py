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

MAX_FILES = 2                           # Set to None to process all valid files
OVERWRITE_EXISTING = True               # True: re-parse and overwrite. False: skip if JSON exists.
SKIP_PREFIXES = ()                      # Clear for now; add prefixes if you notice junk files


# ==========================================
# PARSER CONFIGURATION
# ==========================================

# Sections we actively want to capture people from.
WANTED_SECTIONS = [
    "organizing committee",
    "program committee",
    "steering committee",
    "track chairs",
]

# Sections that tell the parser to stop looking for organizers.
IGNORED_SECTIONS = [
    "advisory board",
    "organizer",
    "organizing partners",
    "table of contents",
    "copyright",
    "full papers",
    "work-in-progress",
    "sponsors",
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
    """Tidies a name: drops surrounding punctuation and collapses spacing."""
    name = name.replace("*", " ").replace("_", " ")
    name = " ".join(name.split()).strip(" ,;:-")
    name = re.sub(r"\.(?=[A-Za-z])", ". ", name)
    return " ".join(name.split())

def is_person_name(name: str) -> bool:
    """Basic validation to ensure the extracted string looks like a human name."""
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
        
    # Normalize unicode to handle weird ligatures
    text = unicodedata.normalize("NFKC", raw_content)
    lines = text.split('\n')
    
    extracted_people = []
    capture = False
    current_section = "Committee Member"
    
    for line in lines:
        line_clean = line.strip()
        if not line_clean:
            continue
            
        # Clean the line for section header detection by removing markdown bolding
        header_check = line_clean.replace("**", "").strip().lower()
        
        # 1. Check if we are entering a wanted section
        if any(wanted in header_check for wanted in WANTED_SECTIONS):
            capture = True
            current_section = header_check.title()
            continue
            
        # 2. Check if we hit a section where we should stop capturing
        if any(ignored in header_check for ignored in IGNORED_SECTIONS):
            capture = False
            continue
            
        # 3. If we are in a valid section, try to extract the person
        if capture:
            # Matches formats like: - **Anatoliy Gruzd**, Ryerson University, Canada — Conference Chair
            # Group 1: Name, Group 2: Association (optional), Group 3: Role (optional)
            match = re.match(r'^[\-\*]\s*\*\*(.*?)\*\*(?:,\s*(.*?))?(?:\s*[—\-\u2014]\s*(.*))?$', line_clean)
            
            if match:
                name = clean_person_name(match.group(1))
                association = match.group(2).strip() if match.group(2) else ""
                
                # If a specific role is on the line (after the dash), use it. Otherwise default to the section name.
                role = match.group(3).strip() if match.group(3) else current_section
                
                if is_person_name(name):
                    extracted_people.append({
                        "name": name,
                        "role": role,
                        "association": association
                    })
                    
    return {
        "conference_file": md_path.name,
        "organizers": extracted_people,
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