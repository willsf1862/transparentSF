from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
from fastapi.staticfiles import StaticFiles
from urllib.parse import quote

import os
import json
from datetime import datetime
from ai_dataprep import process_single_file, create_analysis_map  # Ensure these imports are correct
import pytz
from auto_run import export_for_endpoint  # Ensure this import is correct

import logging

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Capture all levels of logs
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app_debug.log"),  # Log to file
        logging.StreamHandler()  # Log to console
    ]
)

# Create a logger for this module
logger = logging.getLogger(__name__)

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# Define the absolute path to the output directory
script_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(script_dir, 'output')

# Ensure the output directory exists
os.makedirs(output_dir, exist_ok=True)
logger.debug(f"Ensured that output directory exists at '{output_dir}'")

# Mount the output directory to serve static files
app.mount("/output", StaticFiles(directory=output_dir), name="output")
logger.info(f"Mounted '/output' to serve static files from '{output_dir}'")

def find_output_files_for_endpoint(endpoint: str, output_dir: str):
    """
    Search the output directory and its subdirectories for files matching the endpoint.

    Args:
        endpoint (str): The endpoint name to search for in filenames.
        output_dir (str): The absolute path to the output directory.

    Returns:
        dict: A dictionary with keys 'html', 'md', 'txt' and their corresponding file paths.
    """
    output_files = {}
    logger.debug(f"Searching for output files for endpoint: '{endpoint}' in '{output_dir}'")
    for root, dirs, files in os.walk(output_dir):
        for file in files:
            if endpoint in file:
                logger.debug(f"Found file matching endpoint: '{file}' in '{root}'")
                if file.endswith('.html') and 'combined_charts' in file:
                    output_files['html'] = os.path.join(root, file)
                    logger.debug(f"HTML file added: {output_files['html']}")
                elif file.endswith('.md') and 'combined_charts' in file:
                    output_files['md'] = os.path.join(root, file)
                    logger.debug(f"Markdown file added: {output_files['md']}")
                elif file.endswith('_assistant_reply.txt'):
                    output_files['txt'] = os.path.join(root, file)
                    logger.debug(f"TXT log file added: {output_files['txt']}")
    if not output_files:
        logger.warning(f"No output files found for endpoint '{endpoint}'")
    else:
        logger.info(f"Total output files found for endpoint '{endpoint}': {len(output_files)}")
    return output_files

def get_output_file_url(file_path: str, output_dir: str):
    """
    Convert a file path to a URL accessible via the FastAPI app.

    Args:
        file_path (str): The absolute path to the file.
        output_dir (str): The absolute path to the output directory.

    Returns:
        str: The URL to access the file.
    """
    relative_path = os.path.relpath(file_path, output_dir)
    # Ensure proper URL encoding
    url_path = "/output/" + "/".join(quote(part) for part in relative_path.split(os.sep))
    logger.debug(f"Converted file path '{file_path}' to URL '{url_path}'")
    return url_path

def load_and_sort_json():
    datasets = []
    # Get the absolute path to the directory where this script is located
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Construct the path to 'data/datasets/' relative to the script's directory
    datasets_dir = os.path.join(current_dir, 'data', 'datasets')
    
    # Verify the constructed path (optional, for debugging)
    logger.debug(f"Looking for datasets in: {datasets_dir}")
   
    if not os.path.exists(datasets_dir):
        logger.error(f"Datasets directory '{datasets_dir}' does not exist.")
        return datasets
    
    for filename in os.listdir(datasets_dir):
        if filename.endswith('.json'):
            file_path = os.path.join(datasets_dir, filename)
            try:
                with open(file_path, 'r') as file:
                    data = json.load(file)
                    category = data.get('category', 'N/A')
                    endpoint = data.get('endpoint', 'N/A') + '.json'
                    rows_updated_at = data.get('rows_updated_at', 'N/A')
                    try:
                        if rows_updated_at != 'N/A':
                            rows_updated_at = datetime.strptime(rows_updated_at, '%Y-%m-%dT%H:%M:%SZ')
                    except ValueError:
                        rows_updated_at = 'N/A'

                    datasets.append({
                        'filename': filename,
                        'category': category,
                        'endpoint': endpoint,
                        'rows_updated_at': rows_updated_at
                    })
                    logger.debug(f"Loaded dataset: {filename}")
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error in file '{filename}': {e}")
            except Exception as e:
                logger.exception(f"Error loading dataset '{filename}': {e}")

    # Sort datasets by category, then by rows_updated_at
    datasets.sort(key=lambda x: (x['category'], x['rows_updated_at'] if x['rows_updated_at'] != 'N/A' else datetime.min))
    logger.info(f"Total datasets loaded and sorted: {len(datasets)}")
    return datasets

@app.get("/backend")
async def backend(request: Request):
    logger.info("Received request for /backend endpoint")
    datasets_info = load_and_sort_json()
    logger.debug(f"Loaded {len(datasets_info)} datasets")

    # Convert datetime back to string for template rendering
    for dataset in datasets_info:
        if dataset['rows_updated_at'] != 'N/A':
            dataset['rows_updated_at'] = dataset['rows_updated_at'].strftime('%Y-%m-%dT%H:%M:%SZ')
            logger.debug(f"Formatted 'rows_updated_at' for dataset '{dataset['filename']}': {dataset['rows_updated_at']}")
        
        # Extract the endpoint without the .json extension
        endpoint = dataset['endpoint'].replace('.json', '')
        logger.debug(f"Processing dataset '{dataset['filename']}' with endpoint '{endpoint}'")
        
        # Find matching output files for this endpoint
        output_files = find_output_files_for_endpoint(endpoint, output_dir)
        logger.debug(f"Output files found for endpoint '{endpoint}': {output_files}")
        
        # Generate URLs for the output files
        links = {}
        for file_type, path in output_files.items():
            links[file_type] = get_output_file_url(path, output_dir)
            logger.debug(f"Generated URL for '{file_type}': {links[file_type]}")
        
        # Add the links to the dataset information
        dataset['output_links'] = links

    logger.info("Rendering 'backend.html' template with dataset information")
    return templates.TemplateResponse('backend.html', {"request": request, "datasets": datasets_info})

@app.get("/prep-data/{filename}")
async def prep_data(filename: str):
    logger.info(f"Received request to prep data for file: '{filename}'")
    # Get paths
    current_dir = os.path.dirname(__file__)
    datasets_folder = os.path.join(current_dir, 'data', 'datasets')
    output_folder = os.path.join(current_dir, 'data', 'analysis_map')
    error_log = []

    # Check if the file exists
    file_path = os.path.join(datasets_folder, filename)
    if not os.path.exists(file_path):
        logger.error(f"File '{filename}' not found in datasets directory.")
        raise HTTPException(status_code=404, detail=f"File '{filename}' not found in datasets directory.")
    
    try:
        # Call the data preparation function
        logger.debug(f"Processing file '{filename}' with process_single_file")
        process_single_file(filename, datasets_folder, output_folder, datetime.now(pytz.UTC), error_log)
        
        # After successful processing, rebuild the analysis map
        logger.debug("Rebuilding analysis map with create_analysis_map")
        analysis_map_path = create_analysis_map(datasets_folder, output_folder)
        
        if error_log:
            logger.warning(f"Data prepared with warnings for file '{filename}': {error_log}")
            return JSONResponse({
                'status': 'warning', 
                'message': 'Data prepared with warnings and analysis map updated.',
                'analysis_map': analysis_map_path,
                'errors': error_log
            })
            
        # Return success response
        logger.info(f"File '{filename}' prepared successfully and analysis map updated.")
        return JSONResponse({
            'status': 'success', 
            'message': f'File {filename} prepared successfully and analysis map updated.',
            'analysis_map': analysis_map_path
        })
    
    except Exception as e:
        logger.exception(f"Error preparing data for file '{filename}': {str(e)}")
        return JSONResponse({'status': 'error', 'message': str(e)})

@app.get("/run_analysis/{endpoint}")
async def run_analysis(endpoint: str):
    logger.info(f"Received request to run analysis for endpoint: '{endpoint}'")
    error_log = []
    try:
        # Call the analysis function
        logger.debug(f"Executing export_for_endpoint for '{endpoint}'")
        export_for_endpoint(endpoint, output_folder=output_dir, log_file_path=os.path.join(output_dir, 'processing_log.txt'))
        if error_log:
            logger.warning(f"Analysis completed with warnings for endpoint '{endpoint}': {error_log}")
            return JSONResponse({'status': 'warning', 'message': 'Analysis completed with warnings.', 'errors': error_log})
        # Return success response
        logger.info(f"Analysis for endpoint '{endpoint}' completed successfully.")
        return JSONResponse({'status': 'success', 'message': f'Analysis for endpoint {endpoint} completed successfully.'})
    except Exception as e:
        logger.exception(f"Error running analysis for endpoint '{endpoint}': {str(e)}")
        return JSONResponse({'status': 'error', 'message': str(e)})

if __name__ == '__main__':
    import uvicorn
    logger.info("Starting FastAPI application")
    uvicorn.run(app, host="0.0.0.0", port=8000)
