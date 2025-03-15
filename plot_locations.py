import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Set the style for the plots
sns.set(style="whitegrid")

# Read the filtered CSV file
df = pd.read_csv('financial_tech_companies_us.csv')

# Check if we have data
if len(df) == 0:
    print("No data found in the CSV file.")
    exit()

print(f"Total companies in dataset: {len(df)}")
print(f"Industry categories: {df['industry_category'].value_counts().to_dict()}")

# Count companies by location
location_counts = df['locality'].value_counts()

# Get the top 15 locations (if there are more than 15)
top_locations = location_counts.head(15)

# Create a figure with a larger size
plt.figure(figsize=(12, 8))

# Create a bar plot
ax = sns.barplot(x=top_locations.index, y=top_locations.values)

# Add labels and title
plt.title('Top 15 Locations of Financial & Tech Companies in the US with 250+ Employees', fontsize=16)
plt.xlabel('Location', fontsize=14)
plt.ylabel('Number of Companies', fontsize=14)

# Rotate x-axis labels for better readability
plt.xticks(rotation=45, ha='right', fontsize=12)

# Add the count on top of each bar
for i, count in enumerate(top_locations.values):
    ax.text(i, count + 0.5, str(count), ha='center', fontsize=10)

# Adjust layout to make room for labels
plt.tight_layout()

# Save the plot
plt.savefig('financial_tech_companies_location_distribution.png', dpi=300)

# Show the plot
plt.show()

print(f"Plot saved as 'financial_tech_companies_location_distribution.png'")

# Also create a pie chart for the top 10 locations
plt.figure(figsize=(12, 10))

# Get the top 10 locations
top_10_locations = location_counts.head(10)
other_locations = location_counts[10:].sum()

# Create a series with top 10 and "Other"
pie_data = top_10_locations.copy()
if len(location_counts) > 10:
    pie_data['Other'] = other_locations

# Create a pie chart
plt.pie(pie_data, labels=pie_data.index, autopct='%1.1f%%', startangle=90, shadow=True)
plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle
plt.title('Distribution of Financial & Tech Companies by Location (Top 10)', fontsize=16)

# Save the pie chart
plt.savefig('financial_tech_companies_location_pie.png', dpi=300)

# Show the pie chart
plt.show()

print(f"Pie chart saved as 'financial_tech_companies_location_pie.png'")

# Create a pie chart for industry categories
plt.figure(figsize=(10, 8))
industry_counts = df['industry_category'].value_counts()
plt.pie(industry_counts, labels=industry_counts.index, autopct='%1.1f%%', startangle=90, shadow=True)
plt.axis('equal')
plt.title('Distribution of Companies by Industry Category', fontsize=16)
plt.savefig('industry_category_distribution.png', dpi=300)
plt.show()

print(f"Industry category distribution saved as 'industry_category_distribution.png'")

# Print some statistics
print(f"\nTotal number of unique locations: {len(location_counts)}")
print("\nTop 10 locations:")
for location, count in top_10_locations.items():
    print(f"{location}: {count} companies") 