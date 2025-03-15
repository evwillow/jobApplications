import re
import pandas as pd
from bs4 import BeautifulSoup

# ---------- Step 1: Extract the Markdown Table from the README ----------

def extract_table_lines(md_text):
    """
    Extracts lines from the markdown text that are part of the table.
    We check using lstrip() to ignore any leading spaces.
    """
    lines = md_text.splitlines()
    table_lines = [line for line in lines if line.lstrip().startswith('|')]
    return table_lines

def parse_markdown_table(table_lines):
    """
    Parses the markdown table lines into headers and rows.
    Assumes:
      - The first line is the header.
      - The second line is the divider.
      - All subsequent lines are data rows.
    """
    if len(table_lines) < 2:
        raise ValueError("No markdown table found in the file.")
    
    header_line = table_lines[0]
    headers = [cell.strip() for cell in header_line.split('|') if cell.strip()]
    
    # Skip the divider line (the second line)
    data_lines = table_lines[2:]
    rows = []
    for line in data_lines:
        if not line.strip():
            continue
        cells = [cell.strip() for cell in line.split('|') if cell.strip()]
        if len(cells) != len(headers):
            print("Skipping row (unexpected column count):", cells)
            continue
        rows.append(cells)
    return headers, rows

# ---------- Step 2: Define Cleaning Functions ----------

def clean_company(text):
    """
    Extracts both company name and URL from markdown formatting.
    Expected format: **[Company Name](URL)**
    Returns a tuple of (company_name, company_url)
    """
    text = str(text)
    # Match both company name and URL from markdown format
    match = re.search(r'\*\*\[(.*?)\]\((.*?)\)\*\*', text)
    if match:
        return (match.group(1), match.group(2))
    return (text.strip(), None)

def clean_location(text):
    """
    Replaces HTML line breaks (</br>) with a comma and space.
    """
    text = str(text)
    return text.replace('</br>', ', ').strip()

def clean_application_link(text):
    """
    Cleans the Application/Link field.
    
    - If the text contains an HTML <a> tag, uses BeautifulSoup to extract all href attributes.
    - If the text does not contain an <a> tag but starts with "http", assumes it is already a URL.
    - If multiple URLs are present (comma-separated), keeps them all.
    - Otherwise (e.g. if it's "🔒"), returns None to indicate no valid link.
    """
    text = str(text).strip()
    
    # Handle HTML links
    if '<a' in text:
        soup = BeautifulSoup(text, 'html.parser')
        links = [a.get('href') for a in soup.find_all('a') if a.get('href')]
        return ', '.join(links).strip() if links else None
    
    # Handle plain text URLs
    if text.startswith("http"):
        return text.strip()
    
    # Handle comma-separated URLs
    if ',' in text:
        urls = [url.strip() for url in text.split(',') if url.strip().startswith('http')]
        return ', '.join(urls) if urls else None
    
    # No valid link found
    return None

def filter_jobs(df, location_filter=None):
    """
    Filter jobs based on specified criteria.
    
    Args:
        df: pandas DataFrame containing job postings
        location_filter: string to filter locations (e.g. "Remote in USA")
        
    Returns:
        Filtered DataFrame
    """
    if df.empty:
        return df
        
    filtered_df = df.copy()
    
    # Location filter
    if location_filter and 'Location' in filtered_df.columns:
        original_count = len(filtered_df)
        filtered_df = filtered_df[filtered_df['Location'].str.lower().str.contains(location_filter.lower(), na=False)]
        filtered_count = len(filtered_df)
        print(f"Filtered by location '{location_filter}': {original_count - filtered_count} rows removed, {filtered_count} remain.")
    
    return filtered_df

# ---------- Step 3: Process README, Clean, Filter, and Write CSV ----------

def process_readme_to_csv(md_filename, output_csv, location_filter=None):
    # Read the entire markdown file
    try:
        with open(md_filename, 'r', encoding='utf-8') as f:
            md_text = f.read()
    except Exception as e:
        print("Error reading markdown file:", e)
        return

    # Extract table lines from the markdown text
    table_lines = extract_table_lines(md_text)
    print(f"Found {len(table_lines)} table lines.")
    if not table_lines:
        print("No table lines found in the markdown file.")
        return

    # Parse the markdown table into headers and rows
    headers, rows = parse_markdown_table(table_lines)
    print("Parsed headers:", headers)
    print(f"Found {len(rows)} job postings.")

    # Create a pandas DataFrame from the parsed data
    df = pd.DataFrame(rows, columns=headers)

    # Clean the DataFrame columns if they exist
    if 'Company' in df.columns:
        # Split company name and URL into separate columns
        company_data = df['Company'].apply(clean_company)
        df['Company'] = company_data.apply(lambda x: x[0])
        df['Company URL'] = company_data.apply(lambda x: x[1])
        
    if 'Location' in df.columns:
        df['Location'] = df['Location'].apply(clean_location)
    if 'Application/Link' in df.columns:
        df['Application/Link'] = df['Application/Link'].apply(clean_application_link)
    if 'Date Posted' in df.columns:
        df['Date Posted'] = df['Date Posted'].astype(str).str.strip()

    # Reorder columns to put Company URL right after Company
    columns = df.columns.tolist()
    if 'Company URL' in columns:
        columns.remove('Company URL')
        company_idx = columns.index('Company')
        columns.insert(company_idx + 1, 'Company URL')
        df = df[columns]

    # Debug: print unique values in the Location column (lowercase)
    if 'Location' in df.columns:
        unique_locations = df['Location'].str.lower().unique()
        print("Unique Location values found:")
        for loc in unique_locations:
            print(f" - {loc}")

    # Apply filters
    df = filter_jobs(df, location_filter)

    # Save the cleaned and filtered DataFrame to a CSV file
    try:
        df.to_csv(output_csv, index=False)
        print(f"Cleaned data saved to {output_csv}")
    except Exception as e:
        print(f"Error writing {output_csv}: {e}")

if __name__ == '__main__':
    md_filename = 'README.md'   # The markdown file containing your job table
    output_csv = 'jobs.csv'     # The CSV file to create
    location_filter = "Remote in USA"  # Filter for remote jobs in USA, set to None to disable filtering
    process_readme_to_csv(md_filename, output_csv, location_filter)
