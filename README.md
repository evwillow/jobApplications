# Job Applications Tracker

A Python-based system for tracking and managing job applications, with tools for filtering companies, scraping job postings, and processing job data.

## Overview

This project provides a comprehensive solution for job seekers to:
- Filter and identify target companies from large datasets
- Automatically scrape job postings from multiple job sites
- Process and organize job application data
- Track applications and manage the job search process

## Features

- **Company Filtering**: Filter companies by industry, size, and location
- **Job Scraping**: Automated scraping from Indeed, LinkedIn, ZipRecruiter, Glassdoor, and more
- **Data Processing**: Convert job postings from markdown to structured CSV format
- **Remote Job Focus**: Specialized filtering for remote and hybrid positions
- **Salary Filtering**: Focus on high-paying opportunities

## Project Structure

```
jobApplications/
├── filter_companies.py      # Filter companies from CSV data
├── internship_scraper.py    # Scrape job postings from job sites
├── job_postings.py         # Process markdown job data to CSV
├── requirements.txt        # Python dependencies
└── README.md              # This file
```

## Scripts

### filter_companies.py
Filters a CSV file of companies to identify target companies based on:
- Industry (tech and financial sectors)
- Company size (250+ employees)
- Location (US-based companies)

**Usage:**
```bash
python filter_companies.py
```

**Input:** `companies_sorted.csv`
**Output:** `financial_tech_companies_us.csv`

### internship_scraper.py
Scrapes job postings from multiple job sites for companies in your target list.

**Features:**
- Searches across Indeed, LinkedIn, ZipRecruiter, Glassdoor, and Google
- Filters for remote/hybrid positions
- Focuses on internships with competitive salaries
- Avoids duplicate postings
- Rate limiting to prevent blocking

**Usage:**
```bash
python internship_scraper.py
```

**Input:** `financial_tech_companies_us.csv`
**Output:** `internships.csv`

### job_postings.py
Processes markdown job postings and converts them to structured CSV format.

**Features:**
- Extracts job data from markdown tables
- Cleans and standardizes job information
- Filters by location (e.g., remote positions)
- Handles multiple application links

**Usage:**
```bash
python job_postings.py
```

**Input:** `README.md` (with job table)
**Output:** `jobs.csv`

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd jobApplications
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Dependencies

- **pandas**: Data manipulation and analysis
- **matplotlib**: Data visualization
- **seaborn**: Statistical data visualization
- **python-jobspy**: Job scraping library
- **beautifulsoup4**: HTML parsing
- **pathlib**: File path handling

## Usage Workflow

1. **Prepare Company Data**: Start with a CSV file containing company information
2. **Filter Companies**: Run `filter_companies.py` to identify target companies
3. **Scrape Jobs**: Use `internship_scraper.py` to find relevant job postings
4. **Process Data**: Use `job_postings.py` to clean and organize job data
5. **Track Applications**: Use the generated CSV files to track your applications

## Configuration

### Company Filtering
Modify the industry lists in `filter_companies.py`:
```python
tech_industries = [
    'information technology and services',
    'computer software',
    # Add more industries as needed
]
```

### Job Scraping
Adjust search parameters in `internship_scraper.py`:
- `results_wanted`: Number of results per search
- `hours_old`: How recent jobs should be
- `distance`: Search radius for location-based jobs

### Job Processing
Configure location filtering in `job_postings.py`:
```python
location_filter = "Remote in USA"  # Set to None to disable filtering
```

## Output Files

- `financial_tech_companies_us.csv`: Filtered list of target companies
- `internships.csv`: Scraped job postings with details
- `jobs.csv`: Processed job data from markdown sources

## Tips for Success

1. **Start Small**: Begin with a focused list of target companies
2. **Regular Updates**: Run the scraper regularly to catch new postings
3. **Customize Filters**: Adjust salary and location filters based on your preferences
4. **Track Applications**: Use the CSV outputs to maintain your application records
5. **Rate Limiting**: Be respectful of job site rate limits to avoid blocking

## Troubleshooting

### Common Issues

**Scraping Errors**: If you encounter rate limiting or blocking:
- Increase delays between requests
- Reduce batch sizes
- Use different search terms

**Data Quality**: For better results:
- Verify company names in your input CSV
- Check that job sites are accessible
- Review and clean scraped data

**Performance**: For large datasets:
- Process companies in smaller batches
- Use appropriate delays between requests
- Monitor memory usage with large CSV files

## Contributing

This is a personal project for job application tracking. Feel free to fork and modify for your own use.

## License

This project is for personal use. Please respect the terms of service of job sites when scraping data.