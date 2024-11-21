# Reviews tables and columns available and makes decisions about what to analyze for each category
import os
import json
import re
import time
from urllib.parse import urlparse
from openai import OpenAI
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
from dateutil.parser import parse as parse_date
import pytz  # Add this for timezone handling
from swarm import Swarm, Agent
from dateutil.parser import parse as parse_date

# Set key variables 
# Load environment variables
load_dotenv()

# Initialize OpenAI API key
openai_api_key = os.getenv("OPENAI_API_KEY")
if not openai_api_key:
    raise ValueError("OpenAI API key not found in environment variables.")

client = Swarm()

# GPT_MODEL = 'gpt-3.5-turbo-16k'
GPT_MODEL = 'gpt-4'

# ------------------------------
# Utility Functions
# ------------------------------

def sanitize_filename(filename):
    """Sanitize the filename by removing or replacing invalid characters."""
    sanitized = re.sub(r'[<>:"/\\|?*]', '', filename)
    sanitized = sanitized.strip()
    return sanitized

def format_columns(columns):
    """Format the columns information into a readable string."""
    if not columns:
        return ""
    formatted = ""
    for col in columns:
        formatted += f"- {col['name']} ({col['dataTypeName']}): {col['description']}\n"
    return formatted

# ------------------------------
# Main Processing
# ------------------------------

def main():
    # Paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_folder = os.path.join(script_dir, 'data')
    datasets_folder = os.path.join(data_folder, 'datasets')
    output_folder = os.path.join(data_folder, 'analysis_map')

    # Ensure the output directory exists
    os.makedirs(output_folder, exist_ok=True)

    # Verify that the datasets directory exists
    if not os.path.isdir(datasets_folder):
        print(f"Datasets directory not found at {datasets_folder}")
        return

    # List all JSON files in the datasets directory
    article_list = [f for f in os.listdir(datasets_folder) if f.endswith('.json')]

    if not article_list:
        print(f"No JSON files found in {datasets_folder}")
        return

    print(f"Found {len(article_list)} JSON files to process.")

    # Create a single output file for all results
    output_filename = f"filtered_outputs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    output_path = os.path.join(output_folder, output_filename)
    all_outputs = []

    # Define the threshold date (make it offset-aware)
    threshold_date = datetime(2024, 9, 1, tzinfo=pytz.UTC)

    # Process each file individually
    for idx, filename in enumerate(article_list, start=1):
        article_path = os.path.join(datasets_folder, filename)
        print(f"\nProcessing file {idx}/{len(article_list)}: {filename}")

        try:
            with open(article_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            print(f"JSON decode error in file {filename}: {e}")
            continue
        except Exception as e:
            print(f"Error reading file {filename}: {e}")
            continue

        # Check if "rows_updated_at" exists and is later than the threshold date
        rows_updated_at = data.get("rows_updated_at", None)
        if not rows_updated_at:
            print(f"File '{filename}' skipped: 'rows_updated_at' not found.")
            continue

        try:
            # Parse the updated_at_date and make it offset-aware
            updated_at_date = parse_date(rows_updated_at)
            if updated_at_date.tzinfo is None:
                # If the date is offset-naive, make it UTC
                updated_at_date = updated_at_date.replace(tzinfo=pytz.UTC)
            
            # Compare the dates
            if updated_at_date <= threshold_date:
                print(f"File '{filename}' skipped: 'rows_updated_at' is not after {threshold_date}.")
                continue
        except Exception as e:
            print(f"Error parsing 'rows_updated_at' for file '{filename}': {e}")
            continue
        
        # Extract relevant fields
        title = data.get('title', 'Untitled')
        description = data.get('description', '')
        page_text = data.get('page_text', '')
        columns = data.get('columns', [])
        url = data.get('url', '')
        endpoint = data.get('endpoint', '')

        # Format columns
        columns_formatted = format_columns(columns)

        # Construct the prompt
        system_message = """
            You are an AI assistant that generates working API queries and categorizes datasets for analysis.  We are looking to find anomalous trends within the dataset.  Trends are spoting by comparing sums or averages over time by category.  So we are looking for data that updates reugularrly, has at least one time series variable and at least one numeric variable.  Often with city datasets, the numeric variable we need is actually just a count of the number of items by month (like for police or fire reports).

            Your task is:

            - Given dataset information (title, description, columns), generate a working API query with an endpoint and query that will work.
              Construct a SoQL query that selects the following: 
              Whenever selecting fields always take the value of the fieldName key, NOT the name key.
              Date Fields = at least one date field.  Choose the one that is most likely to be of use to an analyst looking at patterns in San Francisco.  If you can't decide, you can take a few different date fields.  Data should be aggregated and grouped by month by default.  Make sure the field name you return matches the one you names in the query.  In the query, if you queried for date_trunc_ym(report_datetime) AS month, then use the name Month here, not date_trunc_ym(report_datetime).
              Numeric Fields You also need at least one numeric field, use sum() to aggregate when grouping.  If you can't see an interesting numeric field, then use count(*) in the query and call that field 'item_count'. If you do that be sure to add 'item_count' to the numericFields you return.
              Category Fields It should alse include a few category fields that will allow you to breakdown the data into useful groups.  Prefer fields that have names as opposed to ids if you see both. 
              Filter for only data from 9/1/2022 forward. 
              Aggregate data by month.
              Remember to use _ characters instead of spaces in field names
              
              Remember to use column functions like date_trunc_y() or date_trunc_ym() for date grouping.
              
                    A SELECT clause with the desired columns.
                    A WHERE clause with exact dates and specified conditions.
                    GROUP BY, ORDER BY clauses as needed.
                    THERE SHOULD NEVER BE A FROM clause in the query.
                    Validate that all columns used in the query exist in the dataset's schema.
                    Make sure the query is properly URL-encoded when needed.

                    
            - Categorize the dataset into one of the following categories: Safety, Health, Economy, Housing, Education, Transportation, Other.
            - Estimate the usefullness on a scale of 1-3 (1=least useful, 3=most useful) for an monthly analysis of trends happening in San Francisco.

            Please provide the output in JSON format, with keys 'DateFields' 'NumericFields' 'CategoryFields' 'endpoint', 'query',  'category', and usefulness.  The 'DateFields', 'NumericFields', and 'CategoryFields' keys should contain lists of OUTPUT field names, meaning the ones that the query will return. 
            The 'endpoint' and 'query' keys should contain the API endpoint and the SoQL query, respectively.  The 'category' key should contain the category name.  The 'usefulness' key should contain the usefulness estimate.

            Ensure that the query is a valid SoQL query, and that the endpoint is correct (the dataset identifier).
            Remember no from clause.  
            
            Here's a good example: 
            endpoint: "wg3w-h783.json",
        query: `
            SELECT 
                supervisor_district, 
                incident_category, 
                incident_subcategory, 
                incident_description, 
                report_type_code, 
                date_trunc_ym(report_datetime) AS month
                COUNT(*) AS count, 
                CASE 
                    WHEN incident_category IN ('Assault', 'Homicide', 'Rape', 'Robbery') THEN 'Violent Crime'
                    WHEN incident_category IN ('Burglary', 'Malicious Mischief', 'Embezzlement', 'Larceny Theft', 'Stolen Property', 'Vandalism', 'Motor Vehicle Theft', 'Arson') THEN 'Property Crime'
                    ELSE 'Other Crime'
                END AS category_group
            WHERE report_datetime>='2022-09-01'
            GROUP BY 
                category_group, 
                incident_category, 
                incident_subcategory, 
                incident_description, 
                report_type_code, 
                supervisor_district, 
                year, 
                month
            ORDER BY 
                category_group, 
                incident_category, 
                incident_subcategory, 
                incident_description, 
                report_type_code, 
                supervisor_district, 
                year, 
                month

            Ensure the output is strictly formatted as valid JSON. Do not include any additional text or explanations outside the JSON block.
            """

        user_message = f"""
        Here is the dataset information:

        Title: {title}

        Endpoint: {endpoint}
        Description:
        {description}

        Columns:
        {columns_formatted}

        """

        # Define the anomaly finder agent   
        analyst_agent = Agent(
            model=GPT_MODEL,
            name="Analyst",
            instructions=system_message,
            functions=[],
            debug=True,
        )
        # Call the OpenAI API
        messages = [{"role": "user", "content": user_message}]
        
        response = client.run(agent=analyst_agent, messages=messages)
        assistant_reply = response.messages[-1]["content"]

        # Extract JSON part using regex
        import re
        json_match = re.search(r'\{.*\}', assistant_reply, re.DOTALL)
        if json_match:
            json_content = json_match.group()
        else:
            raise ValueError("No JSON content found in the assistant's reply.")

        # Attempt to parse the extracted JSON
        try:
            result = json.loads(json_content)
            # Add the filename and title to the result
            result['filename'] = filename
            result['title'] = title
            endpoint=result['endpoint'] 
            query=result['query']
            datefields=result['DateFields']
            print(f"DateFields: {datefields}")
            # set_dataset(endpoint,query)
            all_outputs.append(result)
            print(f"Successfully processed '{title}'.")
            print(result)

        except json.JSONDecodeError as e:
            print(f"Failed to parse assistant's reply as JSON for file '{filename}': {e}")
            print("Assistant's reply:")
            print(assistant_reply)
            # Add the error information to all_outputs
            all_outputs.append({
                'filename': filename,
                'title': title,
                'error': str(e),
                'assistant_reply': assistant_reply
            })
            continue

        except Exception as e:
            print(f"Error processing file '{filename}': {e}")
            # Add the error information to all_outputs
            all_outputs.append({
                'filename': filename,
                'title': title,
                'error': str(e)
            })
            continue

    # Save all outputs to a single JSON file
    with open(output_path, 'w', encoding='utf-8') as out_f:
        json.dump(all_outputs, out_f, indent=4)

    print(f"\nProcessing completed. All outputs saved to '{output_filename}'.")

if __name__ == '__main__':
    main()
