import sys
import os
from webSingle import load_and_combine_climate_data
from tools.genChart import generate_time_series_chart
import pandas as pd
from tools.anomaly_detection import anomaly_detection
from tools.genAggregate import aggregate_data

climate_data=load_and_combine_climate_data()
filtered_data = climate_data

context_variables = {}
html_doc=""

print("Column names in the dataframe:")
print(filtered_data.columns)

context_variables['dataset'] = filtered_data
context_variables['chart_title'] = 'Quantity Issued by Issuance Date'
context_variables['y_axis_label'] = 'Quantity Issued'

chart_html = generate_time_series_chart(
    context_variables,
    time_series_field="Issuance Date",
    numeric_fields=["Quantity Issued"],
    aggregation_period="year",
    max_legend_items=10,
    show_average_line=True
)
markdown_content, html_content = chart_html
html_doc += html_content

context_variables['chart_title'] = 'Quantity Retired by Retirement Date'
context_variables['y_axis_label'] = 'Quantity Retired'
chart_html2 = generate_time_series_chart(
    context_variables,
    time_series_field="Retirement/Cancellation Date",
    numeric_fields=["Quantity Issued"],
    aggregation_period="year",
    group_field="Project Type",
    max_legend_items=10
)
markdown_content, html_content = chart_html2
html_doc += html_content


filter_conditions = [
    {'field': 'Issuance Date', 'operator': '>=', 'value': '2023-10-01'},
    {'field': 'Issuance Date', 'operator': '<=', 'value': '2024-12-01'}
    ]
context_variables['chart_title'] = 'Quantity Issued by Issuance Date by project type'
context_variables['y_axis_label'] = 'Quantity Issued'
chart_html2 = generate_time_series_chart(
    context_variables,
    time_series_field="Issuance Date",
    numeric_fields=["Quantity Issued"],
    aggregation_period="month",
    group_field="Project Type",
    max_legend_items=10,
    filter_conditions=filter_conditions,
)
markdown_content, html_content = chart_html2
html_doc += html_content

context_variables['chart_title'] = 'Credits Issued by sustainability goals '
chart_html2 = generate_time_series_chart(
    context_variables,
    time_series_field="Issuance Date",
    numeric_fields=["Quantity Issued"],
    aggregation_period="month",
    group_field="Additional Certifications",
    max_legend_items=10,
    filter_conditions=filter_conditions
)
markdown_content, html_content = chart_html2
html_doc += html_content

recent_period = {'start': '2024-10-01', 'end': '2024-10-31'}
comparison_period = {'start': '2023-09-01', 'end': '2024-09-30'}
# Aggregate Quantity Issued by month
agg_dataIssued = aggregate_data(
    df=context_variables['dataset'],
    time_series_field='Issuance Date',
    numeric_fields=['Quantity Issued'],
    aggregation_period='year',
    return_html=False
)

agg_dataRetired = aggregate_data(
    df=context_variables['dataset'],
    time_series_field='Retirement/Cancellation Date',
    numeric_fields=['Quantity Issued'],
    aggregation_period='year',
    return_html=False
)
# Combine issued and retired data into a single dataframe
agg_dataIssued = agg_dataIssued.rename(columns={'Quantity Issued': 'Issued'})
agg_dataRetired = agg_dataRetired.rename(columns={'Quantity Issued': 'Retired'})

# Merge the two dataframes on the time period
agg_data = pd.merge(
    agg_dataIssued,
    agg_dataRetired[['time_period', 'Retired']],
    on='time_period',
    how='outer'
)
# Calculate ratio of Issued to Retired credits
agg_data['Issued/Retired Ratio'] = agg_data['Issued'] / agg_data['Retired']

# Replace infinite values (from division by 0) with NaN
agg_data['Issued/Retired Ratio'] = agg_data['Issued/Retired Ratio'].replace([float('inf')], float('nan'))

# Create chart showing the Issued/Retired ratio trend
context_variables['chart_title'] = 'Ratio of Issued to Retired Credits Over Time'
context_variables['y_axis_label'] = 'Issued/Retired Ratio'
context_variables['dataset']=agg_data
chart_html_ratio = generate_time_series_chart(
    context_variables=context_variables,
    time_series_field="time_period",
    numeric_fields=["Issued/Retired Ratio"],
    aggregation_period="year",
    max_legend_items=10
)
markdown_content, html_content = chart_html_ratio
html_doc += html_content

# Fill any NaN values with 0
agg_data = agg_data.fillna(0)

# Sort by time period
agg_data = agg_data.sort_values('time_period')

# Add the aggregation table to the HTML document
if isinstance(agg_data, tuple):
    _, html_table = agg_data
    html_doc += """
    <h2>Monthly Aggregation of Issued Credits</h2>
    """
    html_doc += html_table
else:
    print("Warning: Aggregation did not return HTML table")



# filter_conditions = [
#         {'field': 'Country/Area', 'operator': '==', 'value': 'United States'}
#     ]
context_variables['dataset']=climate_data
# Fix for pt_anomalys
pt_anomalys = anomaly_detection(
    context_variables=context_variables,
    group_field='Project Type',
    filter_conditions=filter_conditions,
    min_diff=2,
    recent_period=recent_period,
    comparison_period=comparison_period,
    date_field='Retirement/Cancellation Date',
    numeric_field='Quantity Issued',
    y_axis_label='Credits Retired',
    title="Anomalies In Retired Credits by Project Type"
)

# Extract 'anomalies' key from the result if available
if 'anomalies' in pt_anomalys:
    html_doc += pt_anomalys['anomalies']
else:
    print("Warning: 'anomalies' key not found in pt_anomalys.")

# Check for Quantity Issued by Project Type
pt_anomalys = anomaly_detection(
    context_variables=context_variables,
    group_field='Project Type',
    filter_conditions=filter_conditions,
    min_diff=2,
    recent_period=recent_period,
    comparison_period=comparison_period,
    date_field='Issuance Date',
    numeric_field='Quantity Issued',
    y_axis_label='Credits Issued',
    title="Anomalies In Issued Credits by Project Type"
)

# Extract 'anomalies' key from the result if available
if 'anomalies' in pt_anomalys:
    html_doc += pt_anomalys['anomalies']
else:
    print("Warning: 'anomalies' key not found in pt_anomalys.")

# Check for Quantity Issued by Country
pt_anomalys = anomaly_detection(
    context_variables=context_variables,
    group_field='Country/Area',
    filter_conditions=filter_conditions,
    min_diff=2,
    recent_period=recent_period,
    comparison_period=comparison_period,
    date_field='Issuance Date',
    numeric_field='Quantity Issued',
    y_axis_label='Credits Issued',
    title="Anomalies In Issued Credits by Country"
)

# Extract 'anomalies' key from the result if available
if 'anomalies' in pt_anomalys:
    html_doc += pt_anomalys['anomalies']
else:
    print("Warning: 'anomalies' key not found in pt_anomalys.")

# Check for Quantity Retiured by Country
pt_anomalys = anomaly_detection(
    context_variables=context_variables,
    group_field='Country/Area',
    filter_conditions=filter_conditions,
    min_diff=2,
    recent_period=recent_period,
    comparison_period=comparison_period,
    date_field='Retirement/Cancellation Date',
    numeric_field='Quantity Issued',
    y_axis_label='Credits Retired',
    title="Anomalies In Retired Credits by Country"
)

# Extract 'anomalies' key from the result if available
if 'anomalies' in pt_anomalys:
    html_doc += pt_anomalys['anomalies']
else:
    print("Warning: 'anomalies' key not found in pt_anomalys.")



output_folder = '/Users/rg/Dropbox/files/TransparentAutoAnalysis/swarm/output'
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

output_file = os.path.join(output_folder, 'analysis_results.html')
with open(output_file, 'w') as file:
    file.write(html_doc)

print(f"HTML document saved to {output_file}")