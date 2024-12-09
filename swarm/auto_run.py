import os
import json
from tools.data_fetcher import set_dataset
from tools.genChart import generate_time_series_chart
from collections import Counter
from tools.anomaly_detection import anomaly_detection
import datetime
import logging
from swarm import Swarm, Agent

# ------------------------------
# Main Processing
# ------------------------------
# Initialize OpenAI API key
openai_api_key = os.getenv("OPENAI_API_KEY")
if not openai_api_key:
    raise ValueError("OpenAI API key not found in environment variables.")

client = Swarm()

# GPT_MODEL = 'gpt-3.5-turbo-16k'
GPT_MODEL = 'gpt-4'

def main():
    # Paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_folder = os.path.join(script_dir, 'data')
    datasets_folder = os.path.join(data_folder, 'analysis_map')
    output_folder = os.path.join(script_dir, 'output')

    # Ensure the output directory exists
    os.makedirs(output_folder, exist_ok=True)

    # Set the range of entries to process
    num_start = 133
    num_end = 136

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

    # Process each file
    if article_list:
        filename = article_list[0]  # Just take the first file since we only have one
        article_path = os.path.join(datasets_folder, filename)
        print(f"\nProcessing file: {filename}")
        try:
            with open(article_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            print(f"JSON decode error in file {filename}: {e}")
            return
        except Exception as e:
            print(f"Error reading file {filename}: {e}")
            return

        # Check if data is a list and sort the records within it
        if isinstance(data, list) and data:
            # Sort the records by category and usefulness
            data.sort(key=lambda x: (str(x.get('report_category', 'Unknown')),
                                     str(x.get('periodic', 'Unknown'))))

            # Save all entries to the output file as a table
            with open(os.path.join(output_folder, 'all_data.html'), 'a', encoding='utf-8') as html_file:
                html_file.write("<table border='1'><tr><th>Index</th><th>Category</th><th>Title</th><th>Usefulness</th></tr>")
                for index, entry in enumerate(data):
                    category = entry.get('report_category', 'Unknown')
                    title = entry.get('title', 'Unknown')
                    usefulness = entry.get('usefulness', 'Unknown')
                    html_file.write(f"<tr><td>{index}</td><td>{category}</td><td>{title}</td><td>{usefulness}</td></tr>")
                html_file.write("</table>")

            # Process entries from num_start to num_end
            for index in range(num_start, num_end):
                print(f"Length of the data: {len(data)}")
                if index >= len(data):
                    break
                data_entry = data[index]
                title = data_entry.get('title', 'Unknown')
                category = data_entry.get('report_category', 'Unknown')
                endpoint = data_entry.get('endpoint', None)
                query = data_entry.get('query', None)
                date_fields = data_entry.get('DateFields', [])
                numeric_fields = data_entry.get('NumericFields', [])
                category_field = data_entry.get('CategoryFields', [])
                # Skip if mandatory fields are missing
                if not query or not endpoint or not date_fields or not numeric_fields:
                    print(f"Missing required fields in entry at index {index}. Skipping.")
                    continue

                # Extract table and column metadata
                table_metadata = data_entry.get('table_metadata', {})
                column_metadata = data_entry.get('column_metadata', [])

                # Create a subfolder for the category in the output folder
                category_folder = os.path.join(output_folder, category)
                os.makedirs(category_folder, exist_ok=True)

                # Set up anomaly detection parameters
                recent_period = {
                    'start': datetime.date(2024, 11, 1),
                    'end': datetime.date(2024, 11, 30)
                }
                comparison_period = {
                    'start': datetime.date(2022, 10, 1),
                    'end': datetime.date(2024, 10, 31)
                }

                # Add these as filter conditions
                filter_conditions = [
                    {'field': date_fields[0], 'operator': '<=', 'value': recent_period['end']},
                    {'field': date_fields[0], 'operator': '>=', 'value': comparison_period['start']},
                ]

                try:
                    context_variables = {}  # Initialize context_variables for each iteration
                    # Set the dataset
                    result = set_dataset(context_variables=context_variables, endpoint=endpoint, query=query)
                    if 'error' in result:
                        print(f"Error setting dataset for entry at index {index}: {result['error']}")
                        continue
                    # Get the dataset from context_variables instead of using result directly
                    if 'dataset' in context_variables:
                        summary = context_variables['dataset'].describe(include='all').to_dict()
                    else:
                        print("No dataset found in context_variables")
                        continue

                    print(summary)
                    all_markdown_contents = []
                    all_html_contents = []
                    print(f"Date fields from JSON: {date_fields}")
                    print(f"Numeric fields from JSON: {numeric_fields}")
                    print(f"Category field from JSON: {category_field}")
                    print(f"Available columns in dataset: {context_variables['dataset'].columns.tolist()}")

                    # Generate charts for each combination of date and numeric fields
                    for date_field in date_fields:
                        for numeric_field in numeric_fields:
                            # Update chart title for this combination
                            context_variables['chart_title'] = (
                                f"{title} - {numeric_field} by {date_field}"
                            )
                            # Generate the chart
                            chart_result = generate_time_series_chart(
                                context_variables=context_variables,
                                time_series_field=date_field,
                                numeric_fields=numeric_field,
                                aggregation_period='month',
                                max_legend_items=10,
                                filter_conditions=filter_conditions,
                                show_average_line=True
                            )
                            # Ensure we're adding strings, not tuples or dicts
                            if isinstance(chart_result, tuple):
                                markdown_content, html_content = chart_result
                                all_markdown_contents.append(str(markdown_content))
                                all_html_contents.append(str(html_content))
                            elif isinstance(chart_result, dict):
                                all_html_contents.append(str(chart_result.get('html', '')))
                            else:
                                all_html_contents.append(str(chart_result))

                    dataset = context_variables['dataset']
                    data_records = dataset.to_dict('records')

                    # Add debug logging to inspect the data structure
                    logging.info(f"Data type: {type(dataset)}")
                    logging.info(f"Columns in dataset: {dataset.columns.tolist()}")
                    logging.info(f"Sample of raw data:")
                    logging.info(f"{dataset.head()}")
                    logging.info(f"\nConverted records sample:")
                    logging.info(f"{data_records[:2]}")  # Show first 2 records

                    logging.info(f"Total records in the dataset: {len(data_records)}")
                    logging.info("First 5 records of filtered data:")

                    # Loop through each category field to detect anomalies
                    for cat_field in category_field:
                        try:
                            context_variables['chart_title'] = (
                                f"{title} - {numeric_fields[0]} by {date_fields[0]} {cat_field}"
                            )
                            chart_result = generate_time_series_chart(
                                context_variables=context_variables,
                                time_series_field=date_fields[0],
                                numeric_fields=numeric_fields[0],
                                aggregation_period='month',
                                max_legend_items=10,
                                group_field=cat_field,
                                filter_conditions=filter_conditions,
                                show_average_line=False
                            )
                            if isinstance(chart_result, tuple):
                                markdown_content, html_content = chart_result
                                all_markdown_contents.append(str(markdown_content))
                                all_html_contents.append(str(html_content))
                            elif isinstance(chart_result, dict):
                                all_html_contents.append(str(chart_result.get('html', '')))
                            else:
                                all_html_contents.append(str(chart_result))

                            # Add debug logging for anomalies
                            print(f"Detecting anomalies for category field: {cat_field}")

                            anomalies_result = anomaly_detection(
                                context_variables=context_variables,
                                group_field=cat_field,
                                filter_conditions=filter_conditions,
                                min_diff=2,
                                recent_period=recent_period,
                                comparison_period=comparison_period,
                                date_field=date_fields[0],
                                numeric_field=numeric_fields[0],
                                y_axis_label=numeric_fields[0],
                            )

                            if anomalies_result and 'anomalies' in anomalies_result:
                                anomalies_html = anomalies_result['anomalies']
                                all_html_contents.append(anomalies_html)
                                print(f"Added anomalies HTML of length {len(anomalies_html)} to output")
                                # Also add markdown content for anomalies
                                anomalies_markdown = anomalies_result.get('anomalies_markdown', '')
                                all_markdown_contents.append(anomalies_markdown)
                                print(f"Added anomalies Markdown of length {len(anomalies_markdown)} to output")
                        except Exception as e:
                            print(f"Error detecting anomalies for category {cat_field} in entry at index {index}: {str(e)}")
                            import traceback
                            print(traceback.format_exc())  # Print full stack trace
                            continue

                    # Combine all charts into single files

                    # Add debug logging for final HTML content
                    print(f"Number of HTML content pieces: {len(all_html_contents)}")
                    print(f"Types of content: {[type(content) for content in all_html_contents]}")

                    # Convert any dictionary content to string and filter out None values
                    processed_html_contents = []
                    for content in all_html_contents:
                        if content is None:
                            continue
                        if isinstance(content, dict):
                            # If it's a dictionary, convert it to a string representation or extract relevant HTML
                            if 'html' in content:
                                processed_html_contents.append(str(content['html']))
                            else:
                                processed_html_contents.append(str(content))
                        else:
                            processed_html_contents.append(str(content))

                    combined_html = "\n\n".join(processed_html_contents)

                    # Prepare metadata for HTML
                    metadata_html = "<h1>Table Metadata</h1>"
                    for key, value in table_metadata.items():
                        metadata_html += f"<p><strong>{key.capitalize()}:</strong> {value}</p>"

                    metadata_html += "<h2>Column Metadata</h2><table border='1'><tr><th>Field Name</th><th>Description</th><th>Data Type</th></tr>"
                    for column in column_metadata:
                        field_name = column.get('fieldName', 'Unknown')
                        description = column.get('description', 'No description')
                        data_type = column.get('dataTypeName', 'Unknown')
                        metadata_html += f"<tr><td>{field_name}</td><td>{description}</td><td>{data_type}</td></tr>"
                    metadata_html += "</table>"

                    # Prepend metadata to combined_html
                    full_html_content = metadata_html + "\n\n" + combined_html

                    # Sanitize title for filename
                    sanitized_title = ''.join(c for c in title if c.isalnum() or c in (' ', '_')).rstrip()

                    # Save combined HTML content
                    html_filename = os.path.join(category_folder, f"{sanitized_title}_{index}_combined_charts.html")
                    with open(html_filename, 'w', encoding='utf-8') as f:
                        f.write(full_html_content)

                    print(f"Saved combined chart files for entry at index {index} in {category_folder}:")
                    print(f"- HTML: {html_filename}")

                    # Process markdown content similar to HTML content
                    processed_markdown_contents = []
                    for content in all_markdown_contents:
                        if content is None:
                            continue
                        if isinstance(content, dict):
                            if 'markdown' in content:
                                processed_markdown_contents.append(str(content['markdown']))
                            else:
                                processed_markdown_contents.append(str(content))
                        else:
                            processed_markdown_contents.append(str(content))

                    combined_markdown = "\n\n".join(processed_markdown_contents)

                    # Prepare metadata for Markdown
                    metadata_md = "# Table Metadata\n"
                    for key, value in table_metadata.items():
                        metadata_md += f"**{key.capitalize()}:** {value}\n\n"

                    metadata_md += "## Column Metadata\n\n| Field Name | Description | Data Type |\n|------------|-------------|-----------|\n"
                    for column in column_metadata:
                        field_name = column.get('fieldName', 'Unknown')
                        description = column.get('description', 'No description')
                        data_type = column.get('dataTypeName', 'Unknown')
                        metadata_md += f"| {field_name} | {description} | {data_type} |\n"

                    # Prepend metadata to combined_markdown
                    full_markdown_content = metadata_md + "\n\n" + combined_markdown

                    markdown_filename = os.path.join(category_folder, f"{sanitized_title}_{index}_combined_charts.md")
                    with open(markdown_filename, 'w', encoding='utf-8') as f:
                        f.write(full_markdown_content)

                    print(f"Saved combined markdown files for entry at index {index} in {category_folder}:")
                    print(f"- Markdown: {markdown_filename}")

                    # Construct the prompt
                    system_message = """
                       Role & Objective:
                        You are a data journalist examining city-level data for San Francisco. Your job is to identify and highlight notable trends and anomalies in the data without speculating on the reasons behind them. Your ultimate goal is to note changes or patterns that might reflect positively—or at times, negatively—on public officials, but always without assigning causation. You’re simply describing what appears to have changed, not why.

                        Tone & Approach:
                        Your voice should be factual, descriptive, and neutral. You may highlight a positive trend (e.g., a decrease in a certain type of crime) in a matter-of-fact way, but do not suggest causation or inject opinions about why it happened. Stick to the data: what it shows, how it compares to previous periods, what improvements or declines are evident.

                        Task:

                        Data Context: You will be provided with the output of an analysis from one specific table. This table focuses on a particular aspect of city life (e.g., crime data in a district, housing permits in a neighborhood, transit ridership stats, etc.).

                        Identify Notable Trends or Anomalies:

                        Look for unusual spikes, declines, or patterns over time.
                        Spot improvements that might be considered good outcomes (e.g., reduced incidents, increased housing production, improved public service metrics).
                        Note any negative shifts as well, but avoid speculation.
                        No Explanation of Causes:

                        Do not try to explain why these changes occurred.
                        Do not assign direct credit or blame to specific officials or conditions. Just note what changed and who might be generally connected to these metrics (e.g., certain police districts, public agencies, or departments).
                        If Nothing Remarkable:

                        If the data is unremarkable—no visible trend, no anomaly—simply state that nothing significant stands out this period.
                        Style for Notes:

                        Your output will be internal notes, not a final article. You can include as many details as you find interesting.
                        Keep your language concise and data-focused, so you can easily pick and choose which insights to use later when writing an actual story.
                        Outcome:
                        A concise but thorough textual summary capturing what is notable (or not) in the provided markdown file. This summary should serve as a quick reference the journalist can review later when deciding which stories to highlight in a monthly report.
                        """

                    user_message = f"""
                    Here is the markdown data:

                    Data: {full_markdown_content}

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
                    assistant_reply_filename = os.path.join(category_folder, f"{sanitized_title}_{index}_assistant_reply.txt")
                    with open(assistant_reply_filename, 'w', encoding='utf-8') as f:
                        f.write(assistant_reply)
                    print(f"Saved assistant reply for entry at index {index} in {category_folder}:")
                    print(f"- Assistant Reply: {assistant_reply_filename}")
                except Exception as e:
                    print(f"Error processing data from entry at index {index}: {e}")
                    import traceback
                    print(traceback.format_exc())  # Print full stack trace
                    continue  # Continue with the next entry

        print(f"\nProcessing complete for entries from index {num_start} to {num_end - 1}.")

if __name__ == '__main__':
    main()
