from tools.genChart import generate_time_series_chart
import os
from tools.retirementdata import read_csv_with_encoding
import pandas as pd


data_folder = 'data/climate'
gsf_file = os.path.join(data_folder, 'GSF.csv')
vera_file = os.path.join(data_folder, 'vcus.csv')

vera_df = read_csv_with_encoding(vera_file)

# Combine the DataFrames
combined_df = pd.concat([vera_df], ignore_index=True)
context_variables = {"dataset": combined_df}

# Remove commas from 'Quantity Issued' values
# combined_df['Quantity Issued'] = combined_df['Quantity Issued'].str.replace(',', '')

# Convert 'Quantity Issued' to numeric
combined_df['Quantity Issued'] = pd.to_numeric(combined_df['Quantity Issued'], errors='coerce')

# Sum the 'Quantity Issued' column
total_quantity_issued = combined_df['Quantity Issued'].sum()
print(f"Total Quantity Issued: {total_quantity_issued}")

# Store the original 'Issuance Date' before conversion
combined_df['Original Issuance Date'] = combined_df['Issuance Date']

# Attempt parsing 'Issuance Date' with multiple formats
def parse_dates(date_str):
    try:
        # Try interpreting with day-first format first
        return pd.to_datetime(date_str, dayfirst=True, errors='coerce')
    except ValueError:
        # If failed, try default parsing (inferring MM/DD/YYYY)
        return pd.to_datetime(date_str, infer_datetime_format=True, errors='coerce')

# Apply the custom parse function
combined_df['Issuance Date'] = combined_df['Issuance Date'].apply(parse_dates)

# Log dates that could not be parsed
invalid_dates = combined_df[combined_df['Issuance Date'].isna()]
print("Invalid 'Issuance Date' values:")
print(invalid_dates[['Original Issuance Date']])

# Generate time series chart
generate_time_series_chart(context_variables, 'Issuance Date', ['Quantity Issued'], 'Year', 'Project Type')
