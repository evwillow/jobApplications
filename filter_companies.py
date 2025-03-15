import pandas as pd

# Load and inspect data
df = pd.read_csv('companies_sorted.csv')
print("Columns:", df.columns.tolist())

# Convert employee estimates to numeric and show basic stats
df['current employee estimate'] = pd.to_numeric(df['current employee estimate'], errors='coerce')
print(f"Total: {len(df)}, With employee data: {df['current employee estimate'].notna().sum()}, " 
      f"With country: {df['country'].notna().sum()}, With industry: {df['industry'].notna().sum()}")

# Define tech and financial industries directly
tech_industries = [
    'information technology and services',
    'computer software',
    'computer hardware',
    'computer networking',
    'semiconductors',
    'internet',
    'telecommunications',
    'wireless',
    'consumer electronics'
]

financial_industries = [
    'financial services',
    'banking',
    'investment banking',
    'insurance',
    'venture capital',
    'accounting',
    'capital markets'
]

# Filter companies: tech or financial industries, over 250 employees, and based in the US
mask = (
    # Industry is in our defined lists
    ((df['industry'].str.lower().isin(tech_industries)) |
     (df['industry'].str.lower().isin(financial_industries))) &
    # Employee count filter
    (df['current employee estimate'] > 249) &
    # US-based companies
    (df['country'].str.contains('^(United States|USA|US)$|united states', case=False, na=False))
)
filtered_df = df[mask]
print(f"Filtered count: {len(filtered_df)}")
print("Unique countries:", filtered_df['country'].unique())
print("Industries in filtered data:\n", filtered_df['industry'].value_counts())

# Save results
output_file = 'financial_tech_companies_us.csv'
filtered_df.to_csv(output_file, index=False)
print(f"Saved {len(filtered_df)} companies to {output_file}")

# Print summary statistics
print("\n--- FILTERING SUMMARY ---")
print(f"Total companies filtered: {len(filtered_df)}")
print(f"Financial industries: {len(filtered_df[filtered_df['industry'].str.lower().isin(financial_industries)])}")
print(f"Tech industries: {len(filtered_df[filtered_df['industry'].str.lower().isin(tech_industries)])}")
print("\nTop Financial Companies by Employee Count:")
financial_companies = filtered_df[filtered_df['industry'].str.lower().isin(financial_industries)]
print(financial_companies.sort_values('current employee estimate', ascending=False)[['name', 'industry', 'current employee estimate', 'linkedin url']].head(10))
print("\nTop Tech Companies by Employee Count:")
tech_companies = filtered_df[filtered_df['industry'].str.lower().isin(tech_industries)]
print(tech_companies.sort_values('current employee estimate', ascending=False)[['name', 'industry', 'current employee estimate', 'linkedin url']].head(10))
