import logging
from pathlib import Path
import pymupdf4llm

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MAX_PDFS_TO_PARSE = 15000  # Limit the number of PDFs processed during this run

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
    pdfs_parsed = 0  # Fixed counter to enforce our testing limit robustly

    # Iterate over the PDF files
    for i, pdf_path in enumerate(pdf_files, 1):
        # Enforce the testing cutoff limit strictly
        if pdfs_parsed >= MAX_PDFS_TO_PARSE:
            logging.info(f"Reached MAX_PDFS_TO_PARSE limit ({MAX_PDFS_TO_PARSE}). Halting run.")
            break

        # Generate the corresponding Markdown path
        md_path = pdf_path.with_suffix('.md')
        
        # Skip if this PDF has already been parsed
        if md_path.exists():
            logging.info(f"[{i}/{len(pdf_files)}] Skipping: {pdf_path.name} (Markdown already exists).")
            total_skipped += 1
            continue

        logging.info(f"[{i}/{len(pdf_files)}] Parsing: {pdf_path.name}")
        
        try:
            markdown_content = pymupdf4llm.to_markdown(doc=str(pdf_path))
            
            with open(md_path, 'w', encoding='utf-8') as f:
                f.write(markdown_content)
                
            logging.info(f"  -> Successfully created: {md_path.name}")
            total_parsed += 1
            pdfs_parsed += 1  
            
        except Exception as e:
            logging.error(f"  -> Failed to parse {pdf_path.name}. Error: {e}")
            # Increment counter even on failure so errors don't cause us to go longer than intended
            pdfs_parsed += 1  

    logging.info(f"\nProcessing Complete. Parsed: {total_parsed} | Skipped: {total_skipped} | Total Limit Checked: {pdfs_parsed}/{MAX_PDFS_TO_PARSE}")

if __name__ == '__main__':
    main()