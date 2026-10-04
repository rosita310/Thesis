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

MAX_FILES = 2000                       
OVERWRITE_EXISTING = True              
SKIP_PREFIXES = ()                      

# ==========================================
# PARSER CONFIGURATION
# ==========================================

WANTED_SECTIONS = [
    "organizing committee", "organising committee", "program committee", "programme committee",
    "programming committee", "steering committee", "track chairs", 
    "organization", "workshop chair", "program chair", "programme chair",
    "general chair", "workshop co-chair", "program co-chair",
    "programme co-chair", "general co-chair", "conference committee",
    "publication chair", "organizing chair", "organising chair", "publicity chair",
    "local organizing chair", "local organising chair", "conference chair", "conference co-chair",
    "technical program chair", "technical committee", "international committee", 
    "committee member", "local arrangements chair", "chairs", "members", 
    "pc co-chairs", "webmaster", "workshop organizers"
]

IGNORED_SECTIONS = [
    "advisory board", "organizing partners",
    "table of contents", "copyright", "full papers",
    "work-in-progress", "sponsors", "author index",
    "message from", "welcome message", "reviewers",
    "additional reviewers", "preface"
]

INSTITUTION_KEYWORDS = [
    'University', 'Academy', 'TU ', 'Institute', 
    'College', 'School', 'Labs', 'Research', 
    'Corporation', 'Inc.', 'National', 'Center', 'Centre',
    'Microsoft', 'Google', 'IBM', 'Alibaba'
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
    
    title_pattern = r'^(?:(?:Assoc\.|Assis\.|Assistant|Associate|Chair)\s+)?(?:Prof\.|Professor|Dr\.|Mr\.|Ms\.|Mrs\.|Miss\.?)(?:\s+Dr\.)?\s+'
    name = re.sub(title_pattern, '', name, flags=re.IGNORECASE)
    
    name = " ".join(name.split()).strip(" ,;:-")
    name = re.sub(r"\.(?=[A-Za-z])", ". ", name)
    return " ".join(name.split())

def clean_association(assoc: str) -> str:
    return re.sub(r'[*_]', '', assoc).strip(" ,;:-")

def is_person_name(name: str) -> bool:
    if not name: return False
    if any(character.isdigit() for character in name): return False
    if not any(character.isalpha() for character in name): return False
    if name.count('(') != name.count(')'): return False
    if any(c in name for c in ['”', '“', '"']): return False
    
    stopwords = {'in', 'of', 'with', 'the', 'and', 'for', 'to', 'this', 'that', 'such', 'as'}
    name_words = set(name.lower().split())
    if stopwords.intersection(name_words): return False
        
    return NAME_MIN_WORDS <= len(name.split()) <= NAME_MAX_WORDS

def split_merged_name(text: str, has_comma: bool):
    text = re.sub(r'(?<!\*)\*(?!\*)', '', text)
    
    if any(kw.lower() in text.lower() for kw in INSTITUTION_KEYWORDS):
        match = re.match(r'^((?:(?:Assoc\.|Assis\.|Assistant|Associate|Chair)\s+)?(?:Prof\.|Professor|Dr\.|Mr\.|Ms\.|Mrs\.|Miss\.)?(?:\s+Dr\.)?\s*[A-Z][A-Za-z\-]+(?:\s+[A-Z]\.?)?(?:\s+[A-Z][A-Za-z\-]+)?)\s+(.*)$', text, re.IGNORECASE)
        if match:
            return match.group(1), match.group(2)
            
    if not has_comma:
        fallback_match = re.match(r'^((?:(?:Assoc\.|Assis\.|Assistant|Associate|Chair)\s+)?(?:Prof\.|Professor|Dr\.|Mr\.|Ms\.|Mrs\.|Miss\.)?(?:\s+Dr\.)?\s*[A-Z][A-Za-z\-]+\s+(?:[A-Z]\.\s+)?[A-Z][A-Za-z\-]+)\s+(.+)$', text, re.IGNORECASE)
        if fallback_match:
            return fallback_match.group(1), fallback_match.group(2)
        
    return text, ""

def is_junk_line(line: str) -> bool:
    line_lower = line.strip().lower()
    if re.match(r'^([ivxlcdm]+|\d+)$', line_lower): return True
    
    alpha_count = sum(1 for c in line if c.isalpha())
    upper_count = sum(1 for c in line if c.isupper())
    if alpha_count > 5 and (upper_count / alpha_count) > 0.8:
        return True
        
    return False

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
    pending_person = None
    
    line_buffer = ""
    
    def finalize_pending():
        nonlocal pending_person, line_buffer
        if pending_person:
            pending_person["association"] = clean_association(" ".join(pending_person["association"].split()))
            extracted_people.append(pending_person)
            pending_person = None
        line_buffer = ""
    
    for original_line in lines:
        line_clean = original_line.strip().replace('\x08', '')
        
        if not line_clean:
            finalize_pending()
            continue
            
        line_clean = re.sub(r'[\.,]+$', '', line_clean).strip()
            
        is_toc_line = bool(re.search(r'\.{3,}', line_clean)) or bool(re.search(r'\s{5,}[ivxlcdm0-9]+\**$', line_clean, re.IGNORECASE))
        if is_toc_line:
            line_buffer = ""
            continue

        is_standalone_header = bool(re.match(r'^#+\s', line_clean)) or bool(re.match(r'^\*\*[^\*]+\*\*\s*$', line_clean))
        
        if is_standalone_header:
            finalize_pending()
            header_check = re.sub(r'[*#]', '', line_clean).strip().lower()
            
            if 'continued' in header_check:
                continue
                
            if any(ignored in header_check for ignored in IGNORED_SECTIONS):
                capture = False
            elif any(wanted in header_check for wanted in WANTED_SECTIONS) and len(header_check.split()) < 20:
                capture = True
                current_section = header_check.title()
                current_inline_role = None
            else:
                capture = False
            continue 
            
        if capture:
            if len(line_clean) < 60 and any(wanted in line_clean.lower() for wanted in WANTED_SECTIONS):
                if ',' not in line_clean and '(' not in line_clean.replace('(s)', '').replace('(S)', ''):
                    finalize_pending()
                    current_inline_role = line_clean.title().replace('(S)', '(s)')
                    continue
                    
            inline_match = re.match(r'^\*\*(.*?)\*\*\s*[:-]?\s*(.*)$', line_clean)
            if inline_match:
                role_cand = inline_match.group(1).strip(" :")
                if len(role_cand) < 60:
                    current_inline_role = role_cand.title()
                    line_clean = inline_match.group(2).strip()

            line_clean = re.sub(r'\([^)]*(?:Fellow|Member)[^)]*\)', '', line_clean, flags=re.IGNORECASE).strip()

            if pending_person:
                unbalanced = pending_person["association"].count('(') - pending_person["association"].count(')')
                if unbalanced > 0:
                    pending_person["association"] += " " + line_clean
                    continue

            # Try parsing with current line and any accumulated buffer fragments
            target_line = (line_buffer + " " + line_clean).strip() if line_buffer else line_clean
            parsed = None
            
            match_cd = re.match(r'^(?:\*\*(.*?):\*\*)?\s*([A-Za-z\.\s\-]+)\s*_*\(+\s*_*([^)]+?)_*\s*\)+_*$', target_line)
            if match_cd:
                role_inline = match_cd.group(1)
                if role_inline:
                    current_inline_role = role_inline.strip().title()
                name = clean_person_name(match_cd.group(2))
                assoc = match_cd.group(3).strip()
                if is_person_name(name):
                    parsed = {"name": name, "role": current_inline_role or current_section, "association": assoc}

            if not parsed:
                match_a = re.match(r'^[\-\*]\s*\*\*(.*?)\*\*(?:,\s*(.*?))?(?:\s*[—\-\u2014]\s*(.*))?$', target_line)
                if match_a:
                    name = clean_person_name(match_a.group(1))
                    assoc = match_a.group(2).strip() if match_a.group(2) else ""
                    role = match_a.group(3).strip() if match_a.group(3) else current_section
                    if is_person_name(name):
                        parsed = {"name": name, "role": role, "association": assoc}
            
            if not parsed:
                if len(target_line) < 150: 
                    parts = target_line.split(',')
                    has_comma = len(parts) > 1
                    name_candidate, merged_assoc = split_merged_name(parts[0].strip(), has_comma)
                    
                    if merged_assoc:
                        assoc = merged_assoc
                        if has_comma:
                            assoc += ", " + ", ".join([p.strip() for p in parts[1:]])
                    else:
                        assoc = ", ".join([p.strip() for p in parts[1:]])
                    
                    name = clean_person_name(name_candidate)
                    if is_person_name(name) and (assoc or has_comma):
                        parsed = {"name": name, "role": current_inline_role or current_section, "association": assoc}

            if parsed:
                finalize_pending()
                pending_person = parsed
            else:
                if pending_person:
                    if not is_junk_line(line_clean):
                        pending_person["association"] += " " + line_clean
                else:
                    # Line didn't parse and no pending person. Keep it in buffer to try with next line.
                    if len(target_line) < 100:
                        line_buffer = target_line
                    else:
                        line_buffer = ""

    finalize_pending() 
    
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