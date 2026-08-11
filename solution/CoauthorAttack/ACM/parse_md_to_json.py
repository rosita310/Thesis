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

MAX_FILES = 5                           # Set to None to process all valid files
OVERWRITE_EXISTING = False              # True: re-parse and overwrite. False: skip if JSON exists.
SKIP_PREFIXES = (                       # Files starting with these prefixes will be skipped
    "ACM Transactions on Graphics",     # Doesn't contain information we want   
    "Proceedings of the ACM on",        # Divergent structure from other journals. Also, these are conference proceedings, not true journals.
)

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
# PARSING LOGIC (Iterative target)
# ==========================================

def parse_markdown_file(md_path: Path) -> dict:
    #   Reads a markdown file and extracts editor information.
    #   This is where the complex, iterative parsing logic will go.
    with open(md_path, "r", encoding="utf-8") as file:
        raw_content = file.read()

    # Apply our cleaning sequence
    content = clean_markdown(raw_content)
    
    # TODO: Implement iterative parsing logic here
    # Expected output structure example:
    extracted_data = {
        "journal_file": md_path.name,
        "editors": [
            # {"name": "John Doe", "role": "Editor-in-Chief", "association": "University of X"}
        ],
        "metadata": {}
    }
    
    return extracted_data

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
            
            # 5. Write to JSON
            with open(json_path, "w", encoding="utf-8") as json_file:
                json.dump(extracted_data, json_file, indent=4, ensure_ascii=False)
                
            logging.info(f"Successfully created: {json_path.name}")
            processed_count += 1
            
        except Exception as e:
            logging.error(f"Failed to parse {md_path.name}. Error: {e}", exc_info=True)
            # Depending on testing needs, you might want to raise the exception to stop execution
            # raise e

    logging.info(f"Run complete. Successfully processed {processed_count} files.\n")

if __name__ == "__main__":
    main()