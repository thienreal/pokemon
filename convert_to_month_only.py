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
    
    # Check if it's a fixed gregorian date
    if "dương" in time_lunar_str:
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
        if "dương" in time_lunar_str:
            return None
        return (month, 1, month, 30)  # Approximate for lunar
    
    return None

def parse_gregorian_month(time_lunar_str):
    """
    Parse gregorian month from strings like "13–15/4 dương lịch", "Tháng 3 dương lịch"
    Returns: month number or None
    """
    time_str = time_lunar_str.replace(" dương lịch", "").replace(" dương", "").strip()
    
    # Handle formats like "13–15/4 dương lịch"
    range_match = re.match(r'(\d+)–(\d+)/(\d+)', time_str)
    if range_match:
        month = int(range_match.group(3))
        return month
    
    # Handle formats like "2/1 dương lịch"
    single_match = re.match(r'(\d+)/(\d+)', time_str)
    if single_match:
        month = int(single_match.group(2))
        return month
    
    # Handle formats like "Tháng 3 dương lịch"
    month_match = re.match(r'Tháng\s+(\d+)', time_str)
    if month_match:
        month = int(month_match.group(1))
        return month
    
    # Handle "Tháng 4–5 dương lịch" or similar
    month_range = re.match(r'Tháng\s+(\d+)–(\d+)', time_str)
    if month_range:
        # Return the start month for month range
        return int(month_range.group(1))
    
    return None

def convert_lunar_to_gregorian_month(lunar_date, year):
    """
    Convert lunar date to gregorian and return YYYY-MM format
    lunar_date: (month, day)
    year: gregorian year
    Returns: YYYY-MM string or None
    """
    try:
        lunar = Lunar(year, lunar_date[0], lunar_date[1], isleap=False)
        solar = Converter.Lunar2Solar(lunar)
        return f"{solar.year}-{str(solar.month).zfill(2)}"
    except:
        return None

def convert_lunar_range_to_gregorian_month(lunar_range, year):
    """
    Convert lunar date range to gregorian month (just take the start month)
    lunar_range: (start_month, start_day, end_month, end_day)
    Returns: YYYY-MM string or None
    """
    if lunar_range is None:
        return None
    
    start_month, start_day, end_month, end_day = lunar_range
    return convert_lunar_to_gregorian_month((start_month, start_day), year)

# Read the CSV file
df = pd.read_csv('/workspaces/pokemon/data/vietnam_festivals_with_years_2018_2024.csv')

# Drop the old detailed date columns, keep only the main columns we need
cols_to_keep = [col for col in df.columns if col not in 
                [f'{prefix}_gregorian_{year}' for year in range(2018, 2026) 
                 for prefix in ['start_date', 'end_date']]]

df = df[cols_to_keep]

# Add new columns for years 2018-2025 with only month info
for year in range(2018, 2026):
    df[f'month_gregorian_{year}'] = None

# Process each row
for idx, row in df.iterrows():
    time_lunar = row['time_lunar']
    
    # Check if it's a gregorian fixed date
    if 'dương' in time_lunar or 'dương lịch' in time_lunar:
        # Extract gregorian month from time_lunar
        month = parse_gregorian_month(time_lunar)
        if month is not None:
            for year in range(2018, 2026):
                df.at[idx, f'month_gregorian_{year}'] = f"{year}-{str(month).zfill(2)}"
    else:
        # It's a lunar date, convert to gregorian month
        lunar_range = parse_lunar_date_range(time_lunar)
        
        if lunar_range is not None:
            for year in range(2018, 2026):
                month_str = convert_lunar_range_to_gregorian_month(lunar_range, year)
                df.at[idx, f'month_gregorian_{year}'] = month_str
        else:
            print(f"Warning: Could not parse '{time_lunar}' in row {idx}")

# Save the modified CSV
df.to_csv('/workspaces/pokemon/data/vietnam_festivals.csv', index=False)
print("Conversion completed! File updated with month-only format (YYYY-MM)")
print(f"\nTotal rows processed: {len(df)}")
print("\nVí dụ:")
print("\nLễ hội Bà Chúa Xứ núi Sam (âm lịch 23–27/4):")
for year in range(2018, 2026):
    month = df.iloc[0][f'month_gregorian_{year}']
    print(f"  {year}: {month}")

print("\nLễ hội Tết Chol Chnam Thmay (dương lịch 13–15/4):")
for year in range(2018, 2026):
    month = df.iloc[4][f'month_gregorian_{year}']
    print(f"  {year}: {month}")
