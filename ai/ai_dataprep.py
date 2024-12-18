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
from tools.data_fetcher import set_dataset

# Set key variables 
# Load environment variables
load_dotenv()
# Initialize OpenAI API key
openai_api_key = os.getenv("OPENAI_API_KEY")
if not openai_api_key:
    raise ValueError("OpenAI API key not found in environment variables.")
client = Swarm()
GPT_MODEL = 'gpt-4-turbo'

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
        formatted += f"- {col['fieldName']} ({col['dataTypeName']}): {col['description']}\n"
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
    error_log_path = os.path.join(output_folder, 'error_log.json')  # Path for error log

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
    print(f"Found {len(article_list)} JSON files to process:")
    for idx, filename in enumerate(article_list):
        print(f"{idx}. {filename}")
    # Create a single output file for all results
    output_filename = f"analysis_map_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    output_path = os.path.join(output_folder, output_filename)

    # Initialize error log
    error_log = []

    # Define the threshold date (make it offset-aware)
    threshold_date = datetime(2024, 9, 1, tzinfo=pytz.UTC)

    # Create a directory for individual results
    individual_results_dir = os.path.join(output_folder, 'individual_results')
    os.makedirs(individual_results_dir, exist_ok=True)

    # Process each file individually
    for idx, filename in enumerate(article_list[373:374]):  
        article_path = os.path.join(datasets_folder, filename)
        print(f"\nProcessing file {idx}/{len(article_list)}: {filename}")

        # At the start of processing loop, check if already processed
        result_filename = f"result_{sanitize_filename(filename)}"
        result_path = os.path.join(individual_results_dir, result_filename)
        
        if os.path.exists(result_path):
            print(f"Skipping already processed file: {filename}")
            continue

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
        # Extract just fieldName and description from columns
        columns = [{
            'fieldName': col['fieldName'],
            'description': col.get('description', ''),
            'dataTypeName': col.get('dataTypeName', '')
        } for col in columns if 'fieldName' in col]
        url = data.get('url', '')
        endpoint = data.get('endpoint', '')

        # Format columns
        columns_formatted = format_columns(columns)

        # Construct the prompt
        system_message = """
          You are an AI assistant that generates working API queries and categorizes datasets for analysis.  We are looking to find anomalous trends within the dataset.  Trends are spoting by comparing sums or averages over time by category.  So we are looking for data that updates reugularrly, has at least one time series variable and at least one numeric variable.  Often with city datasets, the numeric variable we need is actually just a count of the number of items by month (like for police or fire reports).

            Your task is:

            - Given dataset information (title, description, columns), generate a working API query with an endpoint and query that will work.
              The field name syou will select and return are the ones that are the output of the query, not the input, so if you use a date_field called _added_date, but select it as a trunc_ym(_added_date) as month, then you would use the fieldname month. 
              Whenever selecting fields always take the value of the fieldName key, NOT the name key.
              Construct a SoQL query that selects the following:
              Date Fields = at least one date field.  Choose the one that is most likely to be of use to an analyst looking at patterns in San Francisco.  If you can't decide, you can take a few different date fields.  Data should be aggregated and grouped by month by default.  Make sure the field name you return matches the one you names in the query.  In the query, if you queried for date_trunc_ym(report_datetime) AS month, then use the name Month here, not date_trunc_ym(report_datetime).
              Numeric Fields You also need at least one numeric field, use sum() to aggregate when grouping.  If you can't see an interesting numeric field, then use count(*) in the query and call that field 'item_count'. If you do that be sure to add 'item_count' to the numericFields you return.
              Category Fields It should alse include a few category fields that will allow you to breakdown the data into useful groups.  Prefer fields that have names as opposed to ids if you see both. 
              Where clause - Include a where clause that filters for only data from 9/1/2022 forward. 
              Aggregate data by month. 
              Location Fields - If there are fields that indicate locations like supervisor district, neighborhood, zip code, etc, include them in this list of location fields and include them in the query. 
              
              Remember to use _ characters instead of spaces in field names
              
              Remember to use column functions like date_trunc_y() or date_trunc_ym() for date grouping.
              
                    A SELECT clause with the desired columns.
                    A WHERE clause with exact dates and specified conditions.
                    GROUP BY, ORDER BY clauses as needed.
                    THERE SHOULD NEVER BE A FROM clause in the query.
                    Validate that all columns used in the query exist in the dataset's schema.
                    Make sure the query is properly URL-encoded when needed.

                    
            - report_category: Categorize the dataset into one of the following categories: Safety, Health, Economy, Housing, Education, Transportation, Other.
            - usefulness: Estimate the usefullness on a scale of 1-3 (1=least useful, 3=most useful) for an monthly analysis of trends happening in San Francisco.
            - column_metadata: Include metadata about all of the fields in the dataset that you have selected including the field name, description, and dataTypeName.
            - table_metadata: Include metadata about the table including the title, description, endpoint and category.
            - periodic: Boolean: yes if this is the kind of data with constant new entries like police reports, or is it a lookup table like a list of departments or a rarely changing stock, like stop sign locations or wireless cariiers.  
            - item_noun - A row in this tableis a what?  In the example above it is a Police Incident Report. 

            Include a 'whom it may interest' section that explains who would be interested in this data and why.

            Please provide the output in JSON format, with keys 'DateFields' 'NumericFields' 'CategoryFields' 'LocationFields' 'endpoint', 'query',  'report_category',  'usefulness', 'column_metadata', 'table_metadata',  'periodic', 'item_noun', 'whom_it_may_interest'.  The 'DateFields', 'NumericFields', and 'CategoryFields' keys should contain lists of OUTPUT field names, meaning the ones that the query will return. 
            The 'endpoint' and 'query' keys should contain the API endpoint and the SoQL query, respectively.  The 'category' key should contain the category name.  The 'usefulness' key should contain the usefulness estimate.

            Ensure that the query is a valid SoQL query, and that the endpoint is correct (the dataset identifier).
            Remember no from clause. 
            
            Here's an example: 
            {
                "DateFields": [
                    "month"
                ],
                "NumericFields": [
                    "total_payments"
                ],
                "CategoryFields": [
                    "organization_group",
                    "department",
                    "program",
                    "character",
                    "object"
                ],
                "endpoint": "n9pm-xkyq.json",
                "query": "SELECT date_trunc_ym(data_loaded_at) AS month, sum(vouchers_paid) as total_payments, organization_group, department, program, character, object WHERE data_loaded_at >= '2022-09-01T00:00:00.000' GROUP BY month, organization_group, department, program, character, object",
                "report_category": "Economy",
                "column_metadata": [
                    {
                        "fieldName": "month",
                        "description": "Datetime the data was loaded to the open data portal, grouped by month",
                        "dataTypeName": "calendar_date"
                    },
                    {
                        "fieldName": "total_payments",
                        "description": "Total of completed payments to vendors",
                        "dataTypeName": "number"
                    },
                    {
                        "fieldName": "organization_group",
                        "description": "Org Group is a group of Departments",
                        "dataTypeName": "text"
                    },
                    {
                        "fieldName": "department",
                        "description": "Departments are the primary organizational unit used by the City and County of San Francisco",
                        "dataTypeName": "text"
                    },
                    {
                        "fieldName": "program",
                        "description": "A program identifies the services a department provides",
                        "dataTypeName": "text"
                    },
                    {
                        "fieldName": "character",
                        "description": "In the type hierarchy, Character is the highest level",
                        "dataTypeName": "text"
                    },
                    {
                        "fieldName": "object",
                        "description": "In the type hierarchy, Object is the middle level",
                        "dataTypeName": "text"
                    }
                ],
                "table_metadata": {
                    "title": "Vendor Payments (Vouchers)",
                    "description": "The San Francisco Controller's Office maintains a database of payments made to vendors from fiscal year 2007 forward",
                    "endpoint": "n9pm-xkyq.json",
                    "category": "Economy",
                    "periodic": true,
                    "item_noun": "Vendor Payment",
                    "district_level": false,
                    "whom_it_may_interest": "Economists, Data Analysts, City and County controllers, vendors that work with the city, and citizens interested in the city's spending",
                    "filename": "Vendor Payments (Vouchers).json",
                    "data_validated": true,
                    "usefulness": 3,
                },
            }
            Ensure the output is strictly formatted as valid JSON.  No operators or additioonal characters, Do not include any additional text or explanations outside the JSON block.
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

        # Extract JSON part
        json_match = re.search(r'\{.*\}', assistant_reply, re.DOTALL)
        if not json_match:
            print(f"No JSON content found in the assistant's reply for '{title}'.")
            error_log.append({
                'filename': filename,
                'title': title,
                'error': "No JSON content found in the assistant's reply.",
                'assistant_reply': assistant_reply
            })
            continue

        json_content = json_match.group()
        try:
            result = json.loads(json_content)
            # Add the filename and title to the result
            result['filename'] = filename
            result['title'] = title
            endpoint = result.get('endpoint', endpoint)
            query = result.get('query', '')
            datefields = result.get('DateFields', [])
            print(f"DateFields: {datefields}")

            # Attempt validation up to 3 times
            attempt_errors = []
            for attempt in range(3):
                try:
                    test_query = query + " LIMIT 1"
                    data_result = set_dataset({}, endpoint=endpoint, query=test_query)
                    if data_result is None:
                        # If we got None, let's set it to an empty dict so we can safely call .get()
                        data_result = {}
                    data = data_result.get('error') if 'error' in data_result else data_result.get('status')
                    
                    queryURL = data_result.get('queryURL')

                    if data == 'success' and not data_result.get('error'):
                        # Data is valid
                        print(f"Data validated: {data}")
                        result['data_validated'] = True
                        with open(result_path, 'w', encoding='utf-8') as f:
                            json.dump(result, f, indent=4)
                        print(f"Successfully processed and saved '{title}'")
                        print(result)
                        break
                    else:
                        # No data or error
                        if isinstance(data, dict) and 'error' in data:
                            error_msg = data['error']
                        else:
                            error_msg = data if isinstance(data, str) else "Query returned no data"
                        print(f"Attempt {attempt + 1}: {error_msg}")
                        print(f"Endpoint: {endpoint}")
                        print(f"Query: {test_query}")
                        print(f"Query URL: {queryURL}")

                        attempt_errors.append({
                            'attempt': attempt + 1,
                            'endpoint': endpoint,
                            'query': test_query,
                            'queryURL': queryURL,
                            'error': error_msg
                        })
                        
                        # Request a revised query
                        result['data_validated'] = False
                        result['error'] = error_msg
                        messages.append({"role": "user", 
                                         "content": f"The query returned no data. Please revise the query. Error: {error_msg}"})
                        response = client.run(agent=analyst_agent, messages=messages)
                        assistant_reply = response.messages[-1]["content"]
                        json_match = re.search(r'\{.*\}', assistant_reply, re.DOTALL)
                        if json_match:
                            updated_result = json.loads(json_match.group())
                            result.update(updated_result)
                            query = updated_result['query']

                except Exception as e:
                    error_msg = str(e)
                    print(f"Attempt {attempt + 1} failed with error: {error_msg}")
                    # Log attempt details without accessing data_result if it's not defined
                    attempt_errors.append({
                        'attempt': attempt + 1,
                        'endpoint': endpoint,
                        'query': test_query,
                        'queryURL': queryURL if 'queryURL' in locals() else None,
                        'error': error_msg
                    })

                    result['data_validated'] = False 
                    result['error'] = error_msg
                    messages.append({"role": "user",
                                     "content": f"The query failed. Please revise the query. Error: {error_msg}"})
                    response = client.run(agent=analyst_agent, messages=messages)
                    assistant_reply = response.messages[-1]["content"]
                    json_match = re.search(r'\{.*\}', assistant_reply, re.DOTALL)
                    if json_match:
                        updated_result = json.loads(json_match.group())
                        result.update(updated_result)  
                        query = updated_result['query']
            else:
                # If we exit the loop without break, validation failed
                print(f"Failed to validate query for '{title}'.")
                print(result)
                # Add attempt details to error log entry
                error_entry = {
                    'filename': filename,
                    'title': title,
                    'error': "Failed to validate query after 3 attempts",
                    'attempt_details': attempt_errors,
                    'assistant_reply': assistant_reply
                }
                error_log.append(error_entry)

        except json.JSONDecodeError as e:
            print(f"Failed to parse assistant's reply as JSON for file '{filename}': {e}")
            print("Assistant's reply:")
            print(assistant_reply)
            # Add the error information to error_log
            error_log.append({
                'filename': filename,
                'title': title,
                'error': str(e),
                'assistant_reply': assistant_reply
            })
        except Exception as e:
            print(f"Error processing file '{filename}': {e}")
            # Add the error information to error_log
            error_log.append({
                'filename': filename,
                'title': title,
                'error': str(e)
            })

        # Combine all individual results at the end
    all_results = []
    for result_file in os.listdir(individual_results_dir):
        if result_file.startswith('result_'):
            with open(os.path.join(individual_results_dir, result_file), 'r', encoding='utf-8') as f:
                try:
                    result = json.load(f)
                    all_results.append(result)
                except Exception as e:
                    print(f"Error reading {result_file}: {e}")

    # Write combined results
    with open(output_path, 'w', encoding='utf-8') as out_f:
        json.dump(all_results, out_f, indent=4)

    # Append to existing error log instead of overwriting
    existing_errors = []
    if os.path.exists(error_log_path):
        with open(error_log_path, 'r', encoding='utf-8') as err_f:
            try:
                existing_errors = json.load(err_f)
            except Exception as e:
                print(f"Error reading existing error log: {e}")
                existing_errors = []

    existing_errors.extend(error_log)

    with open(error_log_path, 'w', encoding='utf-8') as err_f:
        json.dump(existing_errors, err_f, indent=4)

    print(f"\nProcessing completed. Individual results saved in '{individual_results_dir}'")
    print(f"Combined results saved to '{output_filename}'")
    print(f"Errors logged to '{error_log_path}'")



if __name__ == "__main__":
    main()
