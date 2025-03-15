import pandas as pd
from jobspy import scrape_jobs
from datetime import datetime
import time
import os

def load_companies():
    """Load companies from CSV file."""
    try:
        df = pd.read_csv('financial_tech_companies_us.csv')
        companies = df['name'].dropna().unique()
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
        # Create search terms
        search_term = f'"{company}" intern'
        google_search_term = f'{company} internship jobs remote OR hybrid since:2days'
        print(f"Searching: {company}")
        
        jobs = scrape_jobs(
            site_name=["indeed", "linkedin", "zip_recruiter", "glassdoor", "google", "bayt"],
            search_term=search_term,
            google_search_term=google_search_term,
            location="United States",
            results_wanted=30,  # Reduced for faster results
            job_type="internship",
            hours_old=168,  # 7 days
            country_indeed='USA',
            distance=50,  # Search within 50 miles
            is_remote=True,
            description_format="markdown",
            verbose=0  # Reduced logging
        )
        
        if not jobs.empty:
            # Add source tracking if not present
            if 'site' not in jobs.columns:
                jobs['site'] = 'Unknown'
            
            # Log results by source
            sources = jobs['site'].value_counts()
            print(f"Found {len(jobs)} jobs total:")
            for source, count in sources.items():
                print(f"- {source}: {count}")
        
        return jobs
    except Exception as e:
        print(f"Error: {company} - {str(e)}")
        return pd.DataFrame()

def filter_jobs(jobs_df):
    """Filter jobs for high-paying remote positions."""
    if jobs_df.empty:
        return jobs_df
    
    # First filter for intern in title
    intern_keywords = r'intern\b|internship|interns\b'  # Only match whole words
    title_mask = jobs_df['title'].str.contains(intern_keywords, case=False, na=False)
    intern_jobs = jobs_df[title_mask].copy()
    
    # Then check for remote/hybrid positions
    hybrid_keywords = ['hybrid']
    remote_keywords = ['remote', 'virtual', 'work from home', 'wfh', 'work-from-home']
    
    # Separate masks for remote and hybrid
    is_remote = (
        (intern_jobs['is_remote'] == True) |
        (intern_jobs['location'].str.contains('|'.join(remote_keywords), case=False, na=False)) |
        (intern_jobs['title'].str.contains('|'.join(remote_keywords), case=False, na=False))
    )
    
    is_hybrid = (
        (intern_jobs['location'].str.contains('|'.join(hybrid_keywords), case=False, na=False)) |
        (intern_jobs['title'].str.contains('|'.join(hybrid_keywords), case=False, na=False)) |
        (intern_jobs['description'].str.contains('|'.join(hybrid_keywords), case=False, na=False))
    )
    
    # Combine remote and hybrid masks
    location_mask = is_remote | is_hybrid
    remote_jobs = intern_jobs[location_mask].copy()
    
    # Filter for salary
    salary_mask = (
        ((remote_jobs['interval'] == 'hourly') & (remote_jobs['min_amount'] >= 25)) |
        ((remote_jobs['interval'] == 'yearly') & (remote_jobs['min_amount'] >= 52000)) |
        ((remote_jobs['interval'] == 'monthly') & (remote_jobs['min_amount'] >= 4300)) |
        ((remote_jobs['max_amount'].notna()) & (remote_jobs['max_amount'] >= 52000)) |
        (remote_jobs['company'].str.contains('Google|Microsoft|Amazon|Apple|Meta|IBM|Intel', case=False, na=False))
    )
    
    filtered_jobs = remote_jobs[salary_mask].copy()
    
    if not filtered_jobs.empty:
        # Log results by source
        sources = filtered_jobs['site'].value_counts()
        print(f"Found {len(filtered_jobs)} matching jobs:")
        for source, count in sources.items():
            print(f"- {source}: {count}")
    
    return filtered_jobs

def update_csv(new_jobs, csv_file='internships.csv'):
    """Update the CSV file with new jobs, avoiding duplicates."""
    columns = {
        'title': 'Title',
        'company': 'Company',
        'location': 'Location',
        'min_amount': 'Min Salary',
        'max_amount': 'Max Salary',
        'interval': 'Salary Interval',
        'job_url': 'Apply URL',
        'date_posted': 'Posted Date',
        'is_remote': 'Is Remote',
        'site': 'Source'  # Added source tracking
    }
    
    new_df = new_jobs[columns.keys()].rename(columns=columns)
    
    try:
        if os.path.exists(csv_file):
            existing_df = pd.read_csv(csv_file)
            existing_urls = set(existing_df['Apply URL'])
            new_df = new_df[~new_df['Apply URL'].isin(existing_urls)]
            
            if not new_df.empty:
                updated_df = pd.concat([existing_df, new_df], ignore_index=True)
                updated_df.to_csv(csv_file, index=False)
                print(f"Added {len(new_df)} new jobs")
        else:
            new_df.to_csv(csv_file, index=False)
            print(f"Created file with {len(new_df)} jobs")
    except Exception as e:
        print(f"Error updating CSV: {e}")

def main():
    companies = load_companies()
    if not companies:
        print("No companies loaded. Exiting.")
        return
    
    print(f"Processing {len(companies)} companies...")
    total_matches = 0
    
    # Process companies in smaller batches to avoid rate limiting
    batch_size = 3
    for i in range(0, len(companies), batch_size):
        batch = companies[i:i+batch_size]
        print(f"\nBatch {i//batch_size + 1}/{(len(companies) + batch_size - 1)//batch_size}")
        
        for company in batch:
            jobs = search_company_jobs(company)
            if not jobs.empty:
                filtered_jobs = filter_jobs(jobs)
                if not filtered_jobs.empty:
                    update_csv(filtered_jobs)
                    total_matches += len(filtered_jobs)
            time.sleep(2)  # Increased delay between companies
        
        if i + batch_size < len(companies):
            print("Pausing between batches...")
            time.sleep(5)  # Pause between batches
    
    print(f"\nComplete! Found {total_matches} matching jobs")
    print("Results saved to: internships.csv")

if __name__ == "__main__":
    main()
