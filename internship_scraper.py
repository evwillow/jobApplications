import pandas as pd
from jobspy import scrape_jobs
from datetime import datetime
from pathlib import Path
import time

def load_companies():
    """Load companies from CSV file."""
    try:
        df = pd.read_csv('financial_tech_companies_us.csv')
        # Clean company names and remove duplicates
        companies = df['name'].dropna().unique()
        # Remove any special characters that might affect search
        companies = [str(company).strip().replace('"', '') for company in companies]
        print(f"Loaded {len(companies)} companies from CSV")
        return companies
    except Exception as e:
        print(f"Error loading companies: {e}")
        return []

def write_job_to_file(job, f):
    """Write a single job to the file."""
    f.write(f"\nTitle: {job.get('title', 'N/A')}\n")
    f.write(f"Company: {job.get('company', 'N/A')}\n")
    f.write(f"Location: {job.get('location', 'N/A')}\n")
    
    # Format salary information
    min_amount = job.get('min_amount')
    max_amount = job.get('max_amount')
    interval = job.get('interval', '').lower()
    
    if pd.notna(min_amount) or pd.notna(max_amount):
        if pd.notna(min_amount):
            salary = f"${min_amount:,.0f}"
        else:
            salary = "Salary"
        if pd.notna(max_amount):
            salary += f" - ${max_amount:,.0f}"
        if interval:
            salary += f" per {interval}"
        f.write(f"Salary: {salary}\n")
    
    # Add more job details
    if job.get('description'):
        desc = str(job.get('description'))
        # Look for remote/hybrid mentions in description
        remote_mentions = []
        for keyword in ['remote', 'hybrid', 'virtual', 'work from home']:
            if keyword in desc.lower():
                remote_mentions.append(keyword)
        if remote_mentions:
            f.write(f"Work Type: {', '.join(remote_mentions)}\n")
        
        f.write(f"Description Preview: {desc[:200]}...\n")
    
    f.write(f"Apply: {job.get('job_url', 'N/A')}\n")
    f.write(f"Posted: {job.get('date_posted', 'N/A')}\n")
    f.write("-" * 80 + "\n")
    f.flush()  # Ensure the file is updated immediately

def search_company_jobs(company):
    """Search for jobs at a specific company."""
    try:
        # More flexible search term
        search_term = f'"{company}" intern'  # Simplified to get more results initially
        print(f"Searching with term: {search_term}")
        
        jobs = scrape_jobs(
            site_name=["indeed", "linkedin"],
            search_term=search_term,
            location="United States",  # Changed from "Remote" to get more results
            results_wanted=50,  # Increased results
            job_type="internship",
            hours_old=720,  # Increased to 30 days for more results
            country_indeed='USA',
            verbose=1  # Increased verbosity for debugging
        )
        
        if not jobs.empty:
            print(f"\nFound {len(jobs)} initial jobs for {company}")
            print("\nSample of jobs found:")
            sample = jobs.head(3)
            for _, job in sample.iterrows():
                print(f"\nTitle: {job.get('title')}")
                print(f"Location: {job.get('location')}")
                print(f"Salary: {job.get('min_amount')} - {job.get('max_amount')} {job.get('interval')}")
                print(f"Remote: {job.get('is_remote')}")
        else:
            print(f"No jobs found for {company}")
        
        return jobs
    except Exception as e:
        print(f"Error searching jobs for {company}: {e}")
        return pd.DataFrame()

def filter_jobs(jobs_df):
    """Filter jobs for high-paying remote positions."""
    if jobs_df.empty:
        return jobs_df
        
    print(f"\nDebug - Initial jobs count: {len(jobs_df)}")
    print("\nAvailable columns:", jobs_df.columns.tolist())
    
    # First check for remote/hybrid positions
    remote_keywords = [
        'remote', 'hybrid', 'virtual', 'work from home', 'wfh', 
        'telecommut', 'flexible location', 'work-from-home',
        'remote optional', 'hybrid optional', 'remote eligible',
        'remote first', 'remote-first', 'remote/hybrid', 'anywhere'
    ]
    
    # More lenient remote check
    is_remote_mask = (
        (jobs_df['is_remote'] == True) |
        (jobs_df['location'].str.contains('|'.join(remote_keywords), case=False, na=False)) |
        (jobs_df['title'].str.contains('|'.join(remote_keywords), case=False, na=False)) |
        (jobs_df['description'].str.contains('|'.join(remote_keywords), case=False, na=False))
    )
    
    remote_jobs = jobs_df[is_remote_mask].copy()
    print(f"\nJobs with remote/hybrid options: {len(remote_jobs)}")
    
    if not remote_jobs.empty:
        print("\nSample remote jobs before salary filter:")
        sample = remote_jobs.head(2)
        for _, job in sample.iterrows():
            print(f"\nTitle: {job.get('title')}")
            print(f"Company: {job.get('company')}")
            print(f"Location: {job.get('location')}")
            print(f"Salary: {job.get('min_amount')} - {job.get('max_amount')} {job.get('interval')}")
    
    # More lenient salary filter
    salary_mask = (
        ((remote_jobs['interval'] == 'hourly') & (remote_jobs['min_amount'] >= 25)) |  # Lowered threshold
        ((remote_jobs['interval'] == 'yearly') & (remote_jobs['min_amount'] >= 52000)) |  # Lowered threshold
        ((remote_jobs['interval'] == 'monthly') & (remote_jobs['min_amount'] >= 4300)) |
        ((remote_jobs['max_amount'].notna()) & (remote_jobs['max_amount'] >= 52000)) |  # Check max amount
        # If salary info is missing but it's from a major tech company, include it
        (remote_jobs['company'].str.contains('Google|Microsoft|Amazon|Apple|Meta|IBM|Intel', case=False, na=False))
    )
    
    filtered_jobs = remote_jobs[salary_mask].copy()
    print(f"\nFinal high-paying remote jobs: {len(filtered_jobs)}")
    
    if not filtered_jobs.empty:
        print("\nMatching jobs found:")
        for _, job in filtered_jobs.iterrows():
            print(f"\nTitle: {job.get('title')}")
            print(f"Company: {job.get('company')}")
            print(f"Location: {job.get('location')}")
            print(f"Salary: {job.get('min_amount')} - {job.get('max_amount')} {job.get('interval')}")
    
    return filtered_jobs

def main():
    # Load companies
    companies = load_companies()
    if not companies:
        print("No companies loaded. Exiting.")
        return
    
    print(f"\nLoaded {len(companies)} companies. First few companies:")
    print(companies[:5])
    
    # Use a fixed file name in the main directory
    output_file = 'remote_internships.txt'
    total_matches = 0
    
    print(f"\nSearching for remote internships... Results will be saved to: {output_file}")
    
    # Open file in write mode initially to clear previous results
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("=== High-Paying Remote/Hybrid Internships ===\n")
        f.write(f"Search started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
    
    # Process companies in smaller batches
    batch_size = 3  # Reduced batch size
    for i in range(0, len(companies), batch_size):
        batch = companies[i:i+batch_size]
        print(f"\nProcessing companies {i+1}-{min(i+batch_size, len(companies))} of {len(companies)}")
        
        # Open file in append mode for each batch
        with open(output_file, 'a', encoding='utf-8') as f:
            for company in batch:
                print(f"\nSearching: {company}")
                
                # Search and filter jobs
                jobs = search_company_jobs(company)
                if not jobs.empty:
                    filtered_jobs = filter_jobs(jobs)
                    
                    if not filtered_jobs.empty:
                        # Write company header
                        f.write(f"\n=== {company} ===\n")
                        
                        # Write each job
                        for _, job in filtered_jobs.iterrows():
                            write_job_to_file(job, f)
                            total_matches += 1
                            
                        print(f"Found {len(filtered_jobs)} matching jobs at {company}")
                
                # Pause between companies to avoid rate limiting
                time.sleep(2)
        
        # Longer pause between batches
        if i + batch_size < len(companies):
            print("Pausing between batches...")
            time.sleep(5)
    
    # Write summary at the end
    with open(output_file, 'a', encoding='utf-8') as f:
        f.write(f"\n=== Search Complete ===\n")
        f.write(f"Total companies searched: {len(companies)}\n")
        f.write(f"Total matching jobs found: {total_matches}\n")
        f.write(f"Search completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    print(f"\nSearch complete! Found {total_matches} total matching jobs")
    print(f"Results saved to: {output_file}")

if __name__ == "__main__":
    main()
