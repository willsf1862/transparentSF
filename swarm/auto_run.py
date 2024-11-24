import os
import json
from tools.data_fetcher import set_dataset
from tools.genChart import generate_time_series_chart
from collections import Counter
from tools.anomaly_detection import anomaly_detection
import datetime
import logging
# ------------------------------
# Main Processing
# ------------------------------
context_variables = {}

def main():
    # Paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_folder = os.path.join(script_dir, 'data')
    datasets_folder = os.path.join(data_folder, 'analysis_map')
    output_folder = os.path.join(script_dir, 'output')
    
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

    # Process a single file (change index to test different files)
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
            data.sort(key=lambda x: (str(x.get('category', 'Unknown')), 
                                   str(x.get('usefulness', 'Unknown'))))
            
            # Save all entries to the output file as a table
            with open(os.path.join(output_folder, 'all_data1.html'), 'a', encoding='utf-8') as html_file:
                html_file.write("<table border='1'><tr><th>Index</th><th>Category</th><th>Title</th><th>Usefulness</th></tr>")
                for index, entry in enumerate(data):
                    category = entry.get('category', 'Unknown')
                    title = entry.get('title', 'Unknown')
                    usefulness = entry.get('usefulness', 'Unknown')
                    html_file.write(f"<tr><td>{index}</td><td>{category}</td><td>{title}</td><td>{usefulness}</td></tr>")
                html_file.write("</table>")
            # Process only the first sorted record
            data_entry = data[1]
            title = data_entry.get('title', 'Unknown')
            category = data_entry.get('category', 'Unknown')
            endpoint = data_entry.get('endpoint', None)
            query = data_entry.get('query', None)
            date_fields = data_entry.get('DateFields', [])
            numeric_fields = data_entry.get('NumericFields', [])
            category_field = data_entry.get('CategoryFields', [])  # Keep first category field for now
            print(f"(Date Fields: {date_fields}, Numeric Fields: {numeric_fields}, Category Field: {category_field})")
            print(f"Title: {title}")
            # Skip if mandatory fields are missing
            if not query or not endpoint or not date_fields or not numeric_fields:
                print(f"Missing required fields in file {filename}. Skipping.")
                return

            # Create a subfolder for the category in the output folder
            category_folder = os.path.join(output_folder, category)
            os.makedirs(category_folder, exist_ok=True)
            # Set up anomaly detection parameters
            recent_period = {
                'start': datetime.date(2024, 10, 1),
                'end': datetime.date(2024, 10, 31)
            }
            comparison_period = {
                'start': datetime.date(2022, 9, 1),
                'end': datetime.date(2024, 9, 30)
            }
            
            # Add these as filter conditions
            filter_conditions = [
                {'field': date_fields[0], 'operator': '<=', 'value': recent_period['end']},
                {'field': date_fields[0], 'operator': '>=', 'value': comparison_period['start']},
            ]

            try:
                # Set the dataset
                result = set_dataset(context_variables=context_variables, endpoint=endpoint, query=query)
                if 'error' in result:
                    print(f"Error setting dataset for {filename}: {result['error']}")
                    return
                # Initialize lists to store all chart contents
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
                data = context_variables['dataset']
                data_records = data.to_dict('records')
    
                # Add debug logging to inspect the data structure
                logging.info(f"Data type: {type(data)}")
                logging.info(f"Columns in dataset: {data.columns.tolist()}")
                logging.info(f"Sample of raw data:")
                logging.info(f"{data.head()}")
                logging.info(f"\nConverted records sample:")
                logging.info(f"{data_records[:2]}")  # Show first 2 records
                
                logging.info(f"Total records in the dataset: {len(data_records)}")
                logging.info("First 5 records of filtered data:")
            

     
                

                # Loop through each category field to detect anomalies
                for cat_field in category_field:
                    try:
                        context_variables['chart_title'] = (
                            f" - {numeric_fields[0]} by {date_fields[0]} {cat_field}"
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
                        print(f"Error detecting anomalies for category {cat_field}: {str(e)}")
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
                
                # Save combined HTML content
                html_filename = os.path.join(category_folder, f"{os.path.splitext(title)[0]}_combined_charts.html")
                with open(html_filename, 'w', encoding='utf-8') as f:
                    f.write(combined_html)
                
                print(f"Saved combined chart files for {filename} in {category_folder}:")
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
                markdown_filename = os.path.join(category_folder, f"{os.path.splitext(title)[0]}_combined_charts.md")
                with open(markdown_filename, 'w', encoding='utf-8') as f:
                    f.write(combined_markdown)
                
                print(f"Saved combined markdown files for {filename} in {category_folder}:")
                print(f"- Markdown: {markdown_filename}")

            except Exception as e:
                print(f"Error processing data from file {filename}: {e}")
                return
        else:
            print(f"Unexpected structure in file {filename}. Skipping.")
            return

    print("\nProcessing complete for all files.")


if __name__ == '__main__':
    main()
