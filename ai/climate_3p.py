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

context_variables['chart_title'] = 'Quantity Issues by Additional Certifications'
context_variables['y_axis_label'] = 'Quantity Issued'
chart_html2 = generate_time_series_chart(
    context_variables,
    time_series_field="Issuance Date",
    numeric_fields=["Quantity Issued"],
    aggregation_period="year",
    group_field="Additional Certifications",
    max_legend_items=10
)
markdown_content, html_content = chart_html2
html_doc += html_content



output_folder = '/Users/rg/Dropbox/files/TransparentAutoAnalysis/swarm/output'
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

output_file = os.path.join(output_folder, 'climate_thirdparties.html')
with open(output_file, 'w') as file:
    file.write(html_doc)

print(f"HTML document saved to {output_file}")