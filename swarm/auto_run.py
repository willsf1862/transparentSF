import os
import json
from tools.data_fetcher import set_dataset
from tools.genChart import generate_time_series_chart
from collections import Counter
from tools.anomaly_detection import anomaly_detection

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
            data_entry = data[144]
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
                        chart_html = generate_time_series_chart(
                            context_variables=context_variables,
                            time_series_field=date_field,
                            numeric_fields=numeric_field,
                            aggregation_period='month',
                            max_legend_items=10,
                            filter_conditions=None,
                            show_average_line=True
                        )
                        markdown_content, html_content = chart_html
                        all_markdown_contents.append(markdown_content)
                        all_html_contents.append(html_content)
                
              

                # Set up anomaly detection parameters
                recent_period = {'start': '2024-10', 'end': '2024-10'} 
                comparison_period = {'start': '2023-09', 'end': '2024-09'}

                # Loop through each category field to detect anomalies
                for cat_field in category_field:
                    try:
                        context_variables['chart_title'] = (
                            f" - {numeric_fields[0]} by {date_fields[0]} {cat_field}"
                        )                       
                        chart_html = generate_time_series_chart(
                            context_variables=context_variables,
                            time_series_field=date_fields[0],  # Use first date field
                            numeric_fields=numeric_fields[0],  # Use first numeric field
                            aggregation_period='month',
                            max_legend_items=10,
                            group_field=cat_field,
                            filter_conditions=None,
                            show_average_line=False
                        )
                        markdown_content, html_content = chart_html
                        all_html_contents.append(html_content)

                        
                        anomalies = anomaly_detection(
                            context_variables=context_variables,
                            group_field=cat_field,
                            filter_conditions=[],
                            min_diff=2,
                            recent_period=recent_period,
                            comparison_period=comparison_period,
                            date_field=date_fields[0], # Use first date field
                            numeric_field=numeric_fields[0], # Use first numeric field
                            y_axis_label=numeric_fields[0],
                        )
                        if anomalies:
                            all_html_contents.append(anomalies)
                    except Exception as e:
                        print(f"Error detecting anomalies for category {cat_field}: {e}")
                        continue
                  # Combine all charts into single files
                  
               # ... existing code ...
                # Combine all charts into single files
                combined_html = "\n\n".join(str(content) for content in all_html_contents if isinstance(content, str))
                # Save combined HTML content
                html_filename = os.path.join(category_folder, f"{os.path.splitext(title)[0]}_combined_charts.html")
                with open(html_filename, 'w', encoding='utf-8') as f:
                    f.write(combined_html)
                
                print(f"Saved combined chart files for {filename} in {category_folder}:")
                print(f"- HTML: {html_filename}")

            except Exception as e:
                print(f"Error processing data from file {filename}: {e}")
                return
        else:
            print(f"Unexpected structure in file {filename}. Skipping.")
            return

    print("\nProcessing complete for all files.")


if __name__ == '__main__':
    main()
