import pandas as pd
import re
from datetime import datetime
from lunarcalendar import Converter, Solar, Lunar

def parse_lunar_date_range(time_lunar_str):
    """
    Parse lunar date string and return start and end lunar dates
    Returns: (start_month, start_day, end_month, end_day) or None if cannot parse
    """
    # Remove "âm lịch" and "dương lịch" 
    time_str = time_lunar_str.replace(" âm lịch", "").replace(" dương lịch", "").strip()
    
    # Check if it's a fixed gregorian date (like "13–15/4 dương lịch")
    if "dương" in time_lunar_str or "dương lịch" in time_lunar_str:
        return None  # Will handle separately
    
    # Handle formats like "23–27/4 âm lịch", "10–12/2 âm lịch"
    range_match = re.match(r'(\d+)–(\d+)/(\d+)', time_str)
    if range_match:
        start_day = int(range_match.group(1))
        end_day = int(range_match.group(2))
        month = int(range_match.group(3))
        return (month, start_day, month, end_day)
    
    # Handle formats like "6/2 âm lịch", "15/1 âm lịch"
    single_match = re.match(r'(\d+)/(\d+)', time_str)
    if single_match:
        day = int(single_match.group(1))
        month = int(single_match.group(2))
        return (month, day, month, day)
    
    # Handle formats like "Tháng 4–5 âm lịch" (month range)
    month_range_match = re.match(r'Tháng\s+(\d+)–(\d+)', time_str)
    if month_range_match:
        start_month = int(month_range_match.group(1))
        end_month = int(month_range_match.group(2))
        return (start_month, 1, end_month, 30)  # Approximate
    
    # Handle formats like "Tháng 3 dương lịch" or "Tháng 3 âm lịch"
    single_month_match = re.match(r'Tháng\s+(\d+)', time_str)
    if single_month_match:
        month = int(single_month_match.group(1))
        # Check if it's lunar or gregorian
        if "dương" in time_lunar_str:
            return None
        return (month, 1, month, 30)  # Approximate for lunar
    
    return None

def convert_lunar_to_gregorian(lunar_date, year):
    """
    Convert lunar date to gregorian date
    lunar_date: (month, day)
    year: gregorian year
    """
    try:
        lunar = Lunar(year, lunar_date[0], lunar_date[1], isleap=False)
        solar = Converter.Lunar2Solar(lunar)
        return datetime(solar.year, solar.month, solar.day).strftime('%Y-%m-%d')
    except:
        return None

def convert_lunar_range_to_gregorian(lunar_range, year):
    """
    Convert lunar date range to gregorian
    lunar_range: (start_month, start_day, end_month, end_day)
    """
    if lunar_range is None:
        return None, None
    
    start_month, start_day, end_month, end_day = lunar_range
    
    start_date = convert_lunar_to_gregorian((start_month, start_day), year)
    end_date = convert_lunar_to_gregorian((end_month, end_day), year)
    
    return start_date, end_date

# Read the CSV file
df = pd.read_csv('/workspaces/pokemon/data/vietnam_festivals.csv')

# Add new columns for years 2018-2024
years = list(range(2018, 2025))
for year in years:
    df[f'start_date_gregorian_{year}'] = None
    df[f'end_date_gregorian_{year}'] = None

# Process each row
for idx, row in df.iterrows():
    time_lunar = row['time_lunar']
    
    # Check if it's a gregorian fixed date or lunar date
    if 'dương' in time_lunar or 'dương lịch' in time_lunar:
        # Extract gregorian dates from time_lunar for fixed dates
        continue  # Handle separately
    
    lunar_range = parse_lunar_date_range(time_lunar)
    
    if lunar_range is not None:
        for year in years:
            start_date, end_date = convert_lunar_range_to_gregorian(lunar_range, year)
            df.at[idx, f'start_date_gregorian_{year}'] = start_date
            df.at[idx, f'end_date_gregorian_{year}'] = end_date
    else:
        print(f"Warning: Could not parse '{time_lunar}' in row {idx}")

# Save the modified CSV
df.to_csv('/workspaces/pokemon/data/vietnam_festivals_with_years_2018_2024.csv', index=False)
print("Conversion completed! File saved to vietnam_festivals_with_years_2018_2024.csv")
print(f"\nTotal rows processed: {len(df)}")
print("\nFirst few rows with new columns:")
print(df.iloc[0:3, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]].to_string())
