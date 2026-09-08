import logging
from pathlib import Path
import fitz  

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


MAX_PDFS_TO_PARSE = 15000  # Limit the number of PDFs processed during this run
OVERWRITE_EXISTING = True # True: re-parse and overwrite .md files. False: skip existing .md files


def extract_columnar_text(pdf_path: str) -> str:
    """
    Extracts text from a PDF, forcing a column-by-column reading order
    by physically sorting the text blocks by their X/Y coordinates.
    """
    doc = fitz.open(pdf_path)
    extracted_lines = []
    
    for page in doc:

        # get_text("blocks") returns: (x0, y0, x1, y1, "text", block_no, block_type)
        blocks = page.get_text("blocks")

        # Filter for text blocks only (block_type == 0 ignores images/drawings)
        text_blocks = [b for b in blocks if b[6] == 0]
        
        # Sort blocks into vertical column bins (x0), then top-to-bottom within bins (y0)
        text_blocks.sort(key=lambda b: (round(b[0] / 100), b[1]))
        
        for b in text_blocks:
            extracted_lines.append(b[4].strip())
            
        extracted_lines.append("\n--- PAGE BREAK ---\n")
        
    doc.close()
    return "\n\n".join(extracted_lines)


def main():
    script_dir = Path(__file__).resolve().parent
    data_dir = script_dir / 'data'
    
    if not data_dir.exists():
        logging.error(f"Data directory not found at: {data_dir}")
        return

    # Collect all PDF files in the data folder
    pdf_files = list(data_dir.glob('*.pdf'))
    logging.info(f"Found {len(pdf_files)} PDF files total in {data_dir}")

    total_parsed = 0
    total_skipped = 0
    pdfs_parsed = 0  

    # Iterate over the PDF files
    for i, pdf_path in enumerate(pdf_files, 1):
        if pdfs_parsed >= MAX_PDFS_TO_PARSE:
            logging.info(f"Reached MAX_PDFS_TO_PARSE limit ({MAX_PDFS_TO_PARSE}). Halting run.")
            break

        md_path = pdf_path.with_suffix('.md')
        
        if md_path.exists():
            if OVERWRITE_EXISTING:
                logging.info(f"[{i}/{len(pdf_files)}] Overwriting existing Markdown for: {pdf_path.name}")
            else:
                logging.info(f"[{i}/{len(pdf_files)}] Skipping: {pdf_path.name} (Markdown already exists).")
                total_skipped += 1
                continue

        logging.info(f"[{i}/{len(pdf_files)}] Parsing: {pdf_path.name}")
        
        try:
            content = extract_columnar_text(str(pdf_path))
            
            with open(md_path, 'w', encoding='utf-8') as f:
                f.write(content)
                
            logging.info(f"  -> Successfully created: {md_path.name}")
            total_parsed += 1
            pdfs_parsed += 1  
            
        except Exception as e:
            logging.error(f"  -> Failed to parse {pdf_path.name}. Error: {e}")
            pdfs_parsed += 1  

    logging.info(f"\nProcessing Complete. Parsed: {total_parsed} | Skipped: {total_skipped} | Total Limit Checked: {pdfs_parsed}/{MAX_PDFS_TO_PARSE}")

if __name__ == '__main__':
    main()