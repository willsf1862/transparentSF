import pandas as pd
import chardet
import os
import json

def detect_encoding(file_path, num_bytes=100000):
    with open(file_path, 'rb') as f:
        raw_data = f.read(num_bytes)
    result = chardet.detect(raw_data)
    encoding = result['encoding']
    confidence = result['confidence']
    print(f"Detected encoding for '{os.path.basename(file_path)}': {encoding} (Confidence: {confidence})")
    return encoding

def read_csv_with_encoding(file_path):
    try:
        encoding = detect_encoding(file_path)
        df = pd.read_csv(file_path, encoding=encoding)
        print(f"Successfully loaded '{os.path.basename(file_path)}' with encoding '{encoding}'.\n")
        return df
    except UnicodeDecodeError as e:
        print(f"UnicodeDecodeError while reading '{os.path.basename(file_path)}' with encoding '{encoding}': {e}")
        alternative_encodings = ['windows-1252', 'latin1', 'ISO-8859-1', 'utf-16']
        for alt_enc in alternative_encodings:
            try:
                df = pd.read_csv(file_path, encoding=alt_enc)
                print(f"Successfully loaded '{os.path.basename(file_path)}' with alternative encoding '{alt_enc}'.\n")
                return df
            except UnicodeDecodeError as ex:
                print(f"Failed with alternative encoding '{alt_enc}': {ex}")
        try:
            df = pd.read_csv(file_path, encoding='utf-8', errors='replace')
            print(f"Loaded '{os.path.basename(file_path)}' with 'utf-8' encoding, replacing errors.\n")
            return df
        except Exception as final_e:
            print(f"Failed to load '{os.path.basename(file_path)}' even after handling errors: {final_e}\n")
            return None

# Updated  function
def process_dataset(df, date_col, credits_col, source_name, beneficiary_col=None, methodology_col=None):
    # Check if columns exist
    required_cols = [date_col, credits_col]
    if beneficiary_col:
        required_cols.append(beneficiary_col)
    if methodology_col:
        required_cols.append(methodology_col)
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"Warning: Columns {missing_cols} not found in '{source_name}'. Skipping dataset.")
        return pd.DataFrame(columns=['Date', 'Credits', 'Source'])

    processed_df = df[required_cols].copy()
    rename_dict = {date_col: 'Date', credits_col: 'Credits'}
    if beneficiary_col:
        rename_dict[beneficiary_col] = 'Beneficiary'
    if methodology_col:
        rename_dict[methodology_col] = 'Methodology'
    processed_df.rename(columns=rename_dict, inplace=True)
    processed_df['Source'] = source_name
    
    # Truncate 'Methodology' and 'Beneficiary' fields
    if 'Methodology' in processed_df.columns:
        processed_df['Methodology'] = processed_df['Methodology'].astype(str).str.slice(0, 100)
    if 'Beneficiary' in processed_df.columns:
        processed_df['Beneficiary'] = processed_df['Beneficiary'].astype(str).str.slice(0, 100)

    processed_df['Date'] = pd.to_datetime(processed_df['Date'], errors='coerce')
    processed_df['Credits'] = pd.to_numeric(processed_df['Credits'], errors='coerce')
    processed_df.dropna(subset=['Date', 'Credits'], inplace=True)
    return processed_df

def read_data_files(data_dir, datasets_info):
    dataframes = {}
    for dataset in datasets_info:
        name = dataset['name']
        file_path = os.path.join(data_dir, dataset['file_path'])
        file_type = dataset['file_type']
        if not os.path.exists(file_path):
            print(f"File '{file_path}' does not exist.\n")
            continue
        try:
            if file_type == 'csv':
                df = read_csv_with_encoding(file_path)
            elif file_type == 'excel':
                df = pd.read_excel(file_path)
            else:
                print(f"Unsupported file type '{file_type}' for dataset '{name}'")
                continue
            dataframes[name] = df
            print(f"Successfully loaded '{file_path}'.\n")
        except Exception as e:
            print(f"Failed to load '{file_path}': {e}\n")
    return dataframes

def process_datasets(dataframes, datasets_info):
    processed_dfs = []
    for dataset in datasets_info:
        name = dataset['name']
        if name in dataframes:
            df = dataframes[name]
            processed_df = process_dataset(
                df,
                date_col=dataset['date_col'],
                credits_col=dataset['credits_col'],
                source_name=dataset['source_name'],
                beneficiary_col=dataset.get('beneficiary_col'),
                methodology_col=dataset.get('methodology_col')  # Updated line
            )
            if not processed_df.empty:
                processed_dfs.append(processed_df)
    return processed_dfs

def combine_processed_data(processed_dfs):
    if processed_dfs:
        combined_df = pd.concat(processed_dfs, ignore_index=True)
        combined_df.sort_values('Date', inplace=True)
        combined_df.set_index('Date', inplace=True)
    else:
        print("No data to process.")
        combined_df = pd.DataFrame(columns=['Date', 'Credits', 'Source'])
    return combined_df

def aggregate_data(combined_df, current_date_str):
    # Annual Aggregation (No Date Filter)
    annual_aggregation = combined_df.resample('Y').sum().reset_index()
    annual_aggregation['Date'] = annual_aggregation['Date'].dt.strftime('%Y')

    # Aggregate by year and Source for the annual stacked bar chart (no filtering)
    source_annual_aggregation = combined_df.groupby([pd.Grouper(freq='Y'), 'Source']).sum().reset_index()
    source_annual_aggregation['Date'] = source_annual_aggregation['Date'].dt.strftime('%Y')

    # Monthly Aggregation (Filter Last 5 Full Years)
    current_date = pd.to_datetime(current_date_str)
    cutoff_year = current_date.year - 5
    cutoff_date = pd.to_datetime(f"{cutoff_year}-01-01")

    # Filter the combined_df to include only data from January 1st, 5 years ago (for monthly charts)
    filtered_combined_df = combined_df[combined_df.index >= cutoff_date]

    # Aggregate by month (for filtered data)
    monthly_aggregation = filtered_combined_df.resample('M').sum().reset_index()
    monthly_aggregation['Date'] = monthly_aggregation['Date'].dt.strftime('%Y-%m-%d')

    # Aggregate by Date and Source for stacked bar chart (for filtered data)
    source_aggregation = filtered_combined_df.groupby([pd.Grouper(freq='M'), 'Source']).sum().reset_index()
    source_aggregation['Date'] = source_aggregation['Date'].dt.strftime('%Y-%m-%d')

    return annual_aggregation, source_annual_aggregation, monthly_aggregation, source_aggregation
def aggregate_by_methodology(combined_df):

    # Filter data for the last 5 years
    current_year = pd.to_datetime('now').year
    cutoff_year = current_year - 5
    filtered_df = combined_df[combined_df.index.year >= cutoff_year]

    # Ensure the 'Methodology' column exists
    df_with_methodology = filtered_df.dropna(subset=['Methodology'])

    # Extract the year from the 'Date' index
    df_with_methodology['Year'] = df_with_methodology.index.year

    # Group by 'Year', 'Source', and 'Methodology', aggregate 'Credits'
    methodology_aggregation = df_with_methodology.groupby(['Year', 'Source', 'Methodology']).sum().reset_index()

    return methodology_aggregation

def aggregate_emissions_data(emissions_df):
    # Ensure 'Year' is of type string to match with 'Date' in annual_aggregation
    emissions_df['Year'] = emissions_df['Year'].astype(str)

    # Remove any unintended whitespace from 'Entity' column
    emissions_df['Entity'] = emissions_df['Entity'].str.strip()

    # Filter the DataFrame to include only rows where 'Entity' is 'World'
    femissions_df = emissions_df[emissions_df['Entity'] == 'World']

    # Check if 'Year' column exists in the filtered DataFrame
    if 'Year' not in femissions_df.columns:
        print("Error: 'Year' column is missing in the emissions data.")
        return pd.DataFrame(columns=['Year', 'Emissions'])

    # Aggregate emissions by 'Year' in case there are multiple entries per year
    emissions_aggregation = femissions_df.groupby('Year')['Emissions'].sum().reset_index()

    return emissions_aggregation

def compute_top_beneficiaries(combined_df, target_year, target_month):
    # Filter combined_df for target month and year
    target_df = combined_df[
        (combined_df.index.year == target_year) &
        (combined_df.index.month == target_month)
    ]

    # Initialize a dictionary to store top beneficiaries per source
    top_beneficiaries = {}

    # List of sources with beneficiaries
    sources_with_beneficiaries = ['ACR_Retired', 'CAR_Registry_Retirements', 'Puro_Earth_Registry_Retirements', 'VCUs']

    for source in sources_with_beneficiaries:
        source_df = target_df[target_df['Source'] == source]
        if 'Beneficiary' in source_df.columns:
            # Drop rows with missing Beneficiary
            source_df = source_df.dropna(subset=['Beneficiary'])
            # Group by Beneficiary and sum Credits
            beneficiary_group = source_df.groupby('Beneficiary')['Credits'].sum().reset_index()
            # Sort and take top 10
            top_10 = beneficiary_group.sort_values(by='Credits', ascending=False).head(10)
            top_beneficiaries[source] = top_10
        else:
            print(f"No beneficiary information available for source '{source}'.")
    return top_beneficiaries

def generate_html(annual_aggregation, source_annual_aggregation, monthly_aggregation, source_aggregation, top_beneficiaries, methodology_aggregation):
    # Generate HTML tables for top beneficiaries
    tables_html = ""
    for source, top_df in top_beneficiaries.items():
        if top_df.empty:
            table_html = f"<h2>Top 10 Beneficiaries for {source} in September 2024</h2><p>No data available.</p>"
        else:
            table_html = f"""
            <h2>Top 10 Beneficiaries for {source} in September 2024</h2>
            <table border="1" cellpadding="5" cellspacing="0">
                <thead>
                    <tr>
                        <th>Rank</th>
                        <th>Beneficiary</th>
                        <th>Total Credits Retired</th>
                    </tr>
                </thead>
                <tbody>
            """
            for idx, row in top_df.iterrows():
                rank = idx + 1
                beneficiary = row['Beneficiary']
                credits = row['Credits']
                table_html += f"""
                    <tr>
                        <td>{rank}</td>
                        <td>{beneficiary}</td>
                        <td>{credits:,.0f}</td>
                    </tr>
                """
            table_html += """
                </tbody>
            </table>
            <br/>
            """
        tables_html += table_html

    # Convert DataFrames to JSON strings
    annual_agg_json = annual_aggregation.to_json(orient='records')
    source_annual_agg_json = source_annual_aggregation.to_json(orient='records')
    monthly_agg_json = monthly_aggregation.to_json(orient='records')
    source_agg_json = source_aggregation.to_json(orient='records')

    # Generate charts for each registry's methodologies
    methodology_charts = ""
    registries = methodology_aggregation['Source'].unique()

    for registry in registries:
        registry_data = methodology_aggregation[methodology_aggregation['Source'] == registry]
        registry_json = registry_data.to_json(orient='records')
        chart_div_id = f"methodology_chart_{registry}"

        methodology_charts += f"""
        <div class="chart" id="{chart_div_id}"></div>
        <script>
            var registryData = {registry_json};

            // Convert Year to integer
            registryData.forEach(function(row) {{
                row.Year = parseInt(row.Year, 10);
            }});

            // Extract unique years and methodologies
            var years = [...new Set(registryData.map(row => row.Year))].sort(function(a, b) {{
                return a - b;
            }});
            var methodologies = [...new Set(registryData.map(row => row.Methodology))];

            var traces = methodologies.map(function(methodology) {{
                var yValues = years.map(function(year) {{
                    var dataPoint = registryData.find(function(row) {{
                        return row.Year === year && row.Methodology === methodology;
                    }});
                    return dataPoint ? dataPoint.Credits : 0;
                }});
                return {{
                    x: years,
                    y: yValues,
                    name: methodology,
                    type: 'bar'
                }};
            }});

            var layout = {{
                title: 'Credits Retired by Methodology for {registry} (Last 5 Years)',
                xaxis: {{
                    title: 'Year',
                    type: 'category'
                }},
                yaxis: {{
                    title: 'Total Credits Retired'
                }},
                barmode: 'group',
                bargap: 0.1,
                bargroupgap: 0.2
            }};

            Plotly.newPlot('{chart_div_id}', traces, layout);
        </script>
        """


    # Notes for each chart
    chart_notes = [
        # Notes for the first chart (Annual Total Credits Retired)
        """
        <div class="notes">
            <p><strong>Q:</strong> <b> Scale </b> How many tons have actually been retired in the 20 years since these markets emerged, and what percentage of carbon emissions is that on an annual basis?</p>
            <p><strong>A:</strong> It's now about 150 million tonnes a year being retired at the 6 registires I was able to access.  That number is down from its peak in 2021 of 161M Tons.  Thats out of roughly 35 billion tonnes of co2 from fossil fuel emissions according to data from <a href="https://ourworldindata.org/co2-emissions">ourWorldInData</a> on total Fossil Fuel Emissions.  That amounts to less than 1/2 of 1%. Follow up question: How much does this suggest in $per ton and therefor $$ per year spent on these retirements?</p>
        </div>
        """,
        # Notes for the second chart
        """
        <div class="notes">
             <p><strong>Q:</strong> <b> Players </b> How does this break down between various registries? </p>
            <p><strong>A:</strong>  According to the data I was able to access, the market is dominated by Vera which has been in decline for 3 years, and  GSF a #2 player that has been rising. CAR and ACR seem to be powerted by the same technology, CAR is consistently small, ACR is more bursty. Key follow up question: How many tons have been retired in other registries or perhaps just privately by corporations?  </p>
        </div>
        """,
        # Notes for the third chart
        """
        <div class="notes">
            <p><strong>Q:</strong> <b> Seasonality </b> Does this follow a seasonal pattern? </p>
            <p><strong>A:</strong> Yes, it seems to have a strong Q4 seasonality with Decmenber being the largest month by a factor of nearly 2x the average.  Perhaps companies are offsetting the prior year as the data is complete in time for the annual report?  </p>
        </div>
        """,
        # Notes for the fourth chart
        """
        <div class="notes">
            <!-- Add your notes or Q&A for the fourth chart here -->
        </div>
        """,
        # Notes for the fourth chart
        """
        <div class="notes">
            <!-- Add your notes or Q&A for the fourth chart here -->
        </div>
        """
    ]

    # Create the HTML content
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Transparent Technolgy - VCM Analysis</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 40px;
                background-color: #f9f9f9;
                color: #333;
            }}
            h1 {{
                text-align: center;
                font-size: 2.5em;
                font-weight: bold;
                margin-bottom: 50px;
            }}
            .chart {{
                margin-bottom: 10px;
            }}
            .notes {{
                margin-bottom: 50px;
                background-color: #fff3cd;
                padding: 15px;
                border-left: 6px solid #ffeeba;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-bottom: 50px;
            }}
            th, td {{
                border: 1px solid #dddddd;
                text-align: left;
                padding: 8px;
            }}
            th {{
                background-color: #f2f2f2;
            }}
        </style>
        <!-- Include Plotly.js library here -->
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    </head>
    <body>
        <h1>Transparent Technolgy - VCM Analysis</h1>
        
        <!-- Annual Aggregation Charts -->
        <div class="chart" id="annual_total_credits_chart"></div>
        {chart_notes[0]}
        <div class="chart" id="annual_credits_by_source_chart"></div>
        {chart_notes[1]}
    
        <!-- Monthly Aggregation Charts (Last 5 Years) -->
        <div class="chart" id="total_credits_chart"></div>
        {chart_notes[2]}
        <div class="chart" id="credits_by_source_chart"></div>
        {chart_notes[3]}
        
        <div class="charts">
            {methodology_charts}
        </div>
        {chart_notes[4]}

        {tables_html}
        
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        <script>
            // Annual Total Credits Retired Over Time (No filter)
            var annualCreditsData = {annual_agg_json};
            console.log("Annual Credits Data:", annualCreditsData);

            var annualCreditsTrace = {{
                x: annualCreditsData.map(row => row.Date),
                y: annualCreditsData.map(row => row.Credits),
                type: 'bar',
                name: 'Credits Retired',
                text: annualCreditsData.map(row => {{
                    if (row.Percentage !== null && !isNaN(row.Percentage)) {{
                        return row.Percentage.toFixed(1) + '%';
                    }} else {{
                        return '';
                    }}
                }}),
                textposition: 'auto',
                marker: {{
                    color: 'rgba(26, 118, 255, 0.7)',
                    line: {{
                        color: 'rgba(26, 118, 255, 1)',
                        width: 1
                    }}
                }},
            }};

            var annualEmissionsTrace = {{
                x: annualCreditsData.map(row => row.Date),
                y: annualCreditsData.map(row => row.Emissions),
                type: 'scatter',
                mode: 'lines+markers',
                name: 'Carbon Emissions',
                line: {{
                    color: 'rgba(255, 99, 71, 0.7)'
                }},
            }};

            var data = [annualCreditsTrace, annualEmissionsTrace];

            var layout = {{
                title: 'Annual Total Credits Retired and Carbon Emissions',
                xaxis: {{ title: 'Year', tickmode: 'Linear',dtick:1 }},
                yaxis: {{ title: 'Credits Retired (Tonnes of CO₂)', rangemode: 'tozero' }},
                yaxis2: {{
                    title: 'Carbon Emissions (Tonnes of CO₂)',
                    overlaying: 'y',
                    side: 'right',
                    rangemode: 'tozero'
                }},
                bargap: 0.1,
                bargroupgap: 0.2,
            }};

            Plotly.newPlot('annual_total_credits_chart', data, layout);

            // Annual Total Credits Retired Over Time by Source (No filter)
            var annualSourceCreditsData = {source_annual_agg_json};
            var annualSources = [...new Set(annualSourceCreditsData.map(row => row.Source))];
            var annualSourceTraces = annualSources.map(source => {{
                var filteredData = annualSourceCreditsData.filter(row => row.Source === source);
                return {{
                    x: filteredData.map(row => row.Date),
                    y: filteredData.map(row => row.Credits),
                    name: source,
                    type: 'bar'
                }};
            }});
            var annualSourceCreditsLayout = {{
                title: 'Annual Total Credits Retired by Source',
                xaxis: {{
                    title: 'Year'
                }},
                yaxis: {{
                    title: 'Total Credits Retired'
                }},
                barmode: 'stack',
                bargap: 0.1,
                bargroupgap: 0.2
            }};
            Plotly.newPlot('annual_credits_by_source_chart', annualSourceTraces, annualSourceCreditsLayout);

            // Total Credits Retired Over Time (Filtered to Last 5 Years, Monthly)
            var totalCreditsData = {monthly_agg_json};
            var totalCreditsTrace = {{
                x: totalCreditsData.map(row => row.Date),
                y: totalCreditsData.map(row => row.Credits),
                type: 'bar',
                marker: {{
                    color: 'rgba(55, 128, 191, 0.7)',
                    line: {{
                        color: 'rgba(55, 128, 191, 1)',
                        width: 1
                    }}
                }}
            }};
            var totalCreditsLayout = {{
                title: 'Total Credits Retired Over Time (Last 5 Years)',
                xaxis: {{
                    title: 'Month',
                    type: 'date',
                    tickformat: '%b %Y'
                }},
                yaxis: {{
                    title: 'Total Credits Retired'
                }},
                bargap: 0.1,
                bargroupgap: 0.2
            }};
            Plotly.newPlot('total_credits_chart', [totalCreditsTrace], totalCreditsLayout);

            // Total Credits Retired Over Time by Source (Filtered to Last 5 Years, Monthly Stacked Bar)
            var sourceCreditsData = {source_agg_json};
            var sources = [...new Set(sourceCreditsData.map(row => row.Source))];
            var sourceTraces = sources.map(source => {{
                var filteredData = sourceCreditsData.filter(row => row.Source === source);
                return {{
                    x: filteredData.map(row => row.Date),
                    y: filteredData.map(row => row.Credits),
                    name: source,
                    type: 'bar'
                }};
            }});
            var sourceCreditsLayout = {{
                title: 'Total Credits Retired Over Time by Source (Last 5 Years)',
                xaxis: {{
                    title: 'Month',
                    type: 'date',
                    tickformat: '%b %Y'
                }},
                yaxis: {{
                    title: 'Total Credits Retired'
                }},
                barmode: 'stack',
                bargap: 0.1,
                bargroupgap: 0.2
            }};
            Plotly.newPlot('credits_by_source_chart', sourceTraces, sourceCreditsLayout);
        </script>
    </body>
    </html>
    """
    return html_content

def read_annual_emissions(data_dir):
    emissions_file = os.path.join(data_dir, 'annual_carbon.csv')
    if os.path.exists(emissions_file):
        df = read_csv_with_encoding(emissions_file)
        print(f"Successfully loaded '{emissions_file}'.\n")
        return df
    else:
        print(f"File '{emissions_file}' does not exist.\n")
        return pd.DataFrame(columns=['Year', 'Emissions'])

def main():
    data_dir = "../data/climate"
    current_date_str = "2024-10-11"  # or use datetime.today().strftime('%Y-%m-%d')

    datasets_info = [
        {
            'name': 'ACR_Retired',
            'file_path': 'ACR_Retired.csv',
            'file_type': 'csv',
            'date_col': 'Date Issued (GMT)',
            'credits_col': 'Quantity of Credits',
            'source_name': 'ACR_Retired',
            'beneficiary_col': 'Account Holder',
            'methodology_col' : 'Project Type'
        },
        {
            'name': 'CAR_Registry_Retirements',
            'file_path': 'CAR_Registry_Retirements.csv',
            'file_type': 'csv',
            'date_col': 'Status Effective',
            'credits_col': 'Quantity of Offset Credits',
            'source_name': 'CAR_Registry_Retirements',
            'beneficiary_col': 'Account Holder',
            'methodology_col' : 'Project Type'
        },
        {
            'name': 'GSF_Registry_Credits',
            'file_path': 'GSF.csv',
            'file_type': 'csv',
            'date_col': 'Retirement Date',
            'credits_col': 'Quantity',
            'source_name': 'GSF_Registry_Credits',
            'beneficiary_col': None,  # Assuming no beneficiary column
            'methodology_col' : 'Project Type'
        },
        {
            'name': 'Puro_Earth_Registry_Retirements',
            'file_path': 'Puro_Earth_Registry-Retirement_exports.csv',
            'file_type': 'csv',
            'date_col': 'date',
            'credits_col': 'numberOfCredits',
            'source_name': 'Puro_Earth_Registry_Retirements',
            'beneficiary_col': 'Beneficiary',
             'methodology_col' : 'Methodology'
        },
        {
            'name': 'Social_Carbon_Retirements',
            'file_path': 'Social Carbon Retirement.csv',
            'file_type': 'csv',
            'date_col': 'Creation Date',
            'credits_col': 'Quantity',
            'source_name': 'Social_Carbon_Retirements',
            'beneficiary_col': None,  # Assuming no beneficiary column
            'methodology_col' : 'Asset Type'
        },
        {
            'name': 'VCUs',
            'file_path': 'vcus.csv',
            'file_type': 'csv',
            'date_col': 'Retirement/Cancellation Date',
            'credits_col': 'Quantity Issued',
            'source_name': 'VCUs',
            'beneficiary_col': 'Retirement Beneficiary', 
            'methodology_col' : 'Project Type'
        }
    ]

    # Read main data files
    dataframes = read_data_files(data_dir, datasets_info)

    # Read annual emissions data
    emissions_df = read_annual_emissions(data_dir)
    if not emissions_df.empty:
        print("Emissions Data Loaded:")
        print(emissions_df.head())
        # Aggregate emissions data
        emissions_aggregation = aggregate_emissions_data(emissions_df)
        print("Aggregated Emissions Data:")
        print(emissions_aggregation.head())
    else:
        emissions_aggregation = pd.DataFrame(columns=['Year', 'Emissions'])

    # Process datasets
    processed_dfs = process_datasets(dataframes, datasets_info)

    # Combine processed data
    combined_df = combine_processed_data(processed_dfs)

    # Aggregate data
    annual_aggregation, source_annual_aggregation, monthly_aggregation, source_aggregation = aggregate_data(combined_df, current_date_str)
    # Aggregate by methodology
    
    methodology_aggregation = aggregate_by_methodology(combined_df)
    
    # Merge aggregated emissions data with annual_aggregation
    if not emissions_aggregation.empty:
        # Ensure 'Date' in annual_aggregation is of type string
        annual_aggregation['Date'] = annual_aggregation['Date'].astype(str)
        annual_aggregation = annual_aggregation.merge(emissions_aggregation, left_on='Date', right_on='Year', how='left')
        annual_aggregation.drop('Year', axis=1, inplace=True)
    else:
        annual_aggregation['Emissions'] = None

    # Calculate percentage of credits over emissions
    annual_aggregation['Percentage'] = (annual_aggregation['Credits'] / annual_aggregation['Emissions']) * 100
    annual_aggregation['Percentage'] = annual_aggregation['Percentage'].round(1)  # Round to one decimal place

    # Compute top beneficiaries
    target_year = 2024
    target_month = 9  # September

    top_beneficiaries = compute_top_beneficiaries(combined_df, target_year, target_month)

    # Generate HTML content
    html_content = generate_html(
    annual_aggregation,
    source_annual_aggregation,
    monthly_aggregation,
    source_aggregation,
    top_beneficiaries,
    methodology_aggregation  # Pass the variable here
)

    # Save the HTML content to a file
    output_file = "Centigrade_VCM_Analysis.html"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"Webpage '{output_file}' has been created successfully.")

if __name__ == "__main__":
    main()
