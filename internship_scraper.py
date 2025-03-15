import pandas as pd
from jobspy import scrape_jobs
import csv
from datetime import datetime
import time
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraping.log'),
        logging.StreamHandler()
    ]
)

def setup_output_directory():
    """Create output directory if it doesn't exist."""
    output_dir = Path('job_results')
    output_dir.mkdir(exist_ok=True)
    return output_dir

def load_companies():
    """Load and prepare the list of fintech companies."""
    try:
        df = pd.read_csv('financial_tech_companies_us.csv')
        companies = df['name'].dropna().unique().tolist()
        logging.info(f"Loaded {len(companies)} companies from CSV")
        return companies
    except Exception as e:
        logging.error(f"Error loading companies: {str(e)}")
        return []

def normalize_salary(row):
    """Normalize salary to hourly rate for comparison."""
    try:
        if pd.isna(row['MIN_AMOUNT']):
            return 0
        
        amount = float(row['MIN_AMOUNT'])
        interval = str(row['INTERVAL']).lower() if pd.notna(row['INTERVAL']) else ''
        
        if interval == 'hourly':
            return amount
        elif interval == 'yearly':
            return amount / (40 * 52)  # 40 hours per week, 52 weeks
        elif interval == 'monthly':
            return amount / (40 * 4.33)  # 4.33 weeks per month average
        elif interval == 'weekly':
            return amount / 40
        elif interval == 'daily':
            return amount / 8  # assuming 8-hour workday
        return 0
    except Exception:
        return 0

def is_remote_or_hybrid(row):
    """Check if position is remote or hybrid."""
    try:
        # Check IS_REMOTE flag
        if row['IS_REMOTE']:
            return True
            
        # Check description for remote/hybrid keywords
        description = str(row['DESCRIPTION']).lower()
        title = str(row['TITLE']).lower()
        location = str(row['LOCATION']).lower()
        
        remote_keywords = ['remote', 'hybrid', 'virtual', 'work from home', 'wfh']
        return any(keyword in description or keyword in title or keyword in location 
                  for keyword in remote_keywords)
    except Exception:
        return False

def filter_jobs(jobs_df):
    """Filter jobs based on salary and work arrangement criteria."""
    if jobs_df.empty:
        return jobs_df
    
    try:
        # Add normalized hourly rate column
        jobs_df['HOURLY_RATE'] = jobs_df.apply(normalize_salary, axis=1)
        
        # Filter for positions paying over $30/hour
        salary_mask = jobs_df['HOURLY_RATE'] >= 30
        
        # Filter for remote/hybrid positions
        remote_mask = jobs_df.apply(is_remote_or_hybrid, axis=1)
        
        # Combine filters
        filtered_df = jobs_df[salary_mask & remote_mask].copy()
        
        # Drop temporary column
        filtered_df = filtered_df.drop('HOURLY_RATE', axis=1)
        
        return filtered_df
    except Exception as e:
        logging.error(f"Error filtering jobs: {str(e)}")
        return pd.DataFrame()

def scrape_company_jobs(company):
    """Scrape jobs for a single company with error handling and rate limiting."""
    try:
        search_term = f'"{company}" (intern OR internship)'
        logging.info(f"Searching for: {search_term}")
        
        jobs = scrape_jobs(
            site_name=["linkedin", "indeed", "glassdoor"],
            search_term=search_term,
            results_wanted=100,
            job_type="internship",
            description_format="markdown",
            hours_old=168,  # Last 7 days
            verbose=1
        )
        
        if not jobs.empty:
            jobs['COMPANY_SEARCHED'] = company  # Add original search company
            return jobs
            
    except Exception as e:
        logging.error(f"Error scraping jobs for {company}: {str(e)}")
    
    return pd.DataFrame()

def main():
    # Setup
    output_dir = setup_output_directory()
    companies = load_companies()
    
    if not companies:
        logging.error("No companies loaded. Exiting.")
        return
    
    all_jobs = []
    
    # Process each company
    for i, company in enumerate(companies, 1):
        logging.info(f"Processing company {i}/{len(companies)}: {company}")
        
        jobs_df = scrape_company_jobs(company)
        if not jobs_df.empty:
            all_jobs.append(jobs_df)
        
        # Rate limiting
        if i < len(companies):
            time.sleep(5)  # Wait 5 seconds between companies
    
    if all_jobs:
        # Combine all results
        final_df = pd.concat(all_jobs, ignore_index=True)
        
        # Filter for requirements
        filtered_df = filter_jobs(final_df)
        
        if not filtered_df.empty:
            # Add timestamp to filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = output_dir / f'fintech_internships_{timestamp}.csv'
            
            # Save results
            filtered_df.to_csv(output_file, index=False, quoting=csv.QUOTE_NONNUMERIC, escapechar="\\")
            logging.info(f"Found {len(filtered_df)} matching internship positions")
            logging.info(f"Results saved to {output_file}")
            
            # Save summary
            summary = filtered_df.groupby('COMPANY_SEARCHED').size().reset_index(name='positions_found')
            summary_file = output_dir / f'summary_{timestamp}.csv'
            summary.to_csv(summary_file, index=False)
            logging.info(f"Summary saved to {summary_file}")
        else:
            logging.warning("No positions matched the filtering criteria")
    else:
        logging.warning("No jobs found for any company")

if __name__ == "__main__":
    main()
