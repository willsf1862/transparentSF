import json
import os
import re
import requests
from datetime import datetime

def sanitize_filename(filename):
    """Sanitize the filename by removing or replacing invalid characters."""
    sanitized = re.sub(r'[<>:"/\\|?*]', '', filename)
    sanitized = sanitized.strip()
    return sanitized

def scrape_dataset_metadata(dataset_url):
    """Retrieve dataset metadata including columns and descriptions via the Socrata API."""
    # Extract the dataset identifier from the URL
    match = re.search(r'/([a-z0-9]{4}-[a-z0-9]{4})(?:/|$)', dataset_url)
    if not match:
        print(f"Could not extract dataset ID from URL: {dataset_url}")
        return None
    dataset_id = match.group(1)

    metadata_url = f'https://data.sfgov.org/api/views/{dataset_id}.json'

    response = requests.get(metadata_url)

    if response.status_code == 200:
        data = response.json()
        # Get the title and description
        title = data.get('name', 'Untitled')
        category = data.get('category', '')
        description = data.get('description', '')
        # Get the columns
        columns_info = []
        columns = data.get('columns', [])
        for column in columns:
            column_info = {
                'name': column.get('name'),
                'fieldName': column.get('fieldName'),
                'dataTypeName': column.get('dataTypeName'),
                'description': column.get('description'),
                'position': column.get('position'),
                'renderTypeName': column.get('renderTypeName'),
                'tableColumnId': column.get('id'),
            }
            columns_info.append(column_info)
        
        # Get publishing department
        publishing_department = data.get('metadata', {}).get('custom_fields', {}).get('Department Metrics', {}).get('Publishing Department', '')
        # Get most recent update date
        rows_updated_at = data.get('rowsUpdatedAt')
        if rows_updated_at:
            rows_updated_at = datetime.utcfromtimestamp(rows_updated_at).isoformat() + 'Z'
        else:
            rows_updated_at = ''

        dataset_info = {
            'category': category,
            'endpoint': dataset_id,
            'url': dataset_url,
            'title': title,
            'description': description,
            'columns': columns_info,
            'publishing_department': publishing_department,
            'rows_updated_at': rows_updated_at
        }
        return dataset_info
    else:
        print(f"Failed to retrieve metadata: {response.status_code}")
        return None

def main():
    # Load all dataset URLs from the dataset_urls.json file
    data_folder = 'data'
    output_folder = 'datasets'

    # Ensure the output directory exists
    os.makedirs(output_folder, exist_ok=True)

    # Load the dataset URLs
    dataset_urls_file = os.path.join(data_folder, 'dataset_urls.json')
    with open(dataset_urls_file, 'r', encoding='utf-8') as f:
        dataset_urls = json.load(f)

    # Process each URL
    for dataset_url in dataset_urls:
        try:
            print(f"Processing URL: {dataset_url}")
            dataset_info = scrape_dataset_metadata(dataset_url)
            if dataset_info:
                # Use the endpoint (dataset ID) as the filename instead of the title
                endpoint = dataset_info.get('endpoint')
                filename = f"{endpoint}.json"
                output_path = os.path.join(output_folder, filename)
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(dataset_info, f, ensure_ascii=False, indent=4)
                print(f"Saved dataset info to {output_path}")
            else:
                print(f"Failed to process dataset at {dataset_url}")
        except Exception as e:
            print(f"Error processing {dataset_url}: {e}")

if __name__ == '__main__':
    main()
