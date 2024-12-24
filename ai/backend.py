from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
from fastapi.staticfiles import StaticFiles
from urllib.parse import quote

import os
import json
from datetime import datetime
import pytz

from ai_dataprep import process_single_file, create_analysis_map  # Ensure these imports are correct
from auto_run import export_for_endpoint  # Ensure this import is correct

import logging

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Capture all levels
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app_debug.log"),  # Log to file
        logging.StreamHandler()                # Log to console
    ]
)

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
    Returns a dict with keys 'html', 'md', 'txt'.
    """
    output_files = {}
    logger.debug(f"Searching for output files for endpoint: '{endpoint}' in '{output_dir}'")
    for root, dirs, files in os.walk(output_dir):
        for file in files:
            if endpoint in file:
                logger.debug(f"Found file matching endpoint: '{file}' in '{root}'")
                if file.endswith('.html'):
                    output_files['html'] = os.path.join(root, file)
                elif file.endswith('.md'):
                    output_files['md'] = os.path.join(root, file)
                elif file.endswith('_assistant_reply.txt'):
                    output_files['txt'] = os.path.join(root, file)

    return output_files


def get_output_file_url(file_path: str, output_dir: str):
    """
    Convert a file path to a URL accessible via the FastAPI app.
    """
    relative_path = os.path.relpath(file_path, output_dir)
    url_path = "/output/" + "/".join(quote(part) for part in relative_path.split(os.sep))
    return url_path


def load_and_sort_json():
    """
    Loads all .json files from data/datasets, extracts category, endpoint,
    periodic (Yes/No), and last updated (formatted as YY.MM.DD), 
    then sorts them by category, then by last updated date.
    """
    datasets = []
    current_dir = os.path.dirname(os.path.abspath(__file__))
    datasets_dir = os.path.join(current_dir, 'data', 'datasets')
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
                    endpoint = data.get('endpoint', 'N/A')
                    periodic = data.get('periodic', False)  # Assume bool in JSON
                    periodic_str = "Yes" if periodic else "No"

                    rows_updated_at_str = data.get('rows_updated_at', 'N/A')
                    rows_updated_dt = None

                    if rows_updated_at_str != 'N/A':
                        try:
                            # parse the date
                            dt_parsed = datetime.strptime(rows_updated_at_str, '%Y-%m-%dT%H:%M:%SZ')
                            # store the datetime for sorting
                            rows_updated_dt = dt_parsed
                            # store the formatted date (YY.MM.DD) for display
                            rows_updated_at_str = dt_parsed.strftime('%y.%m.%d')
                        except ValueError:
                            rows_updated_at_str = 'N/A'
                            rows_updated_dt = None

                    datasets.append({
                        'filename': filename,
                        'category': category,
                        'periodic': periodic_str,
                        'endpoint': endpoint,
                        'rows_updated_at': rows_updated_at_str,
                        'rows_updated_dt': rows_updated_dt
                    })
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error in file '{filename}': {e}")
            except Exception as e:
                logger.exception(f"Error loading dataset '{filename}': {e}")

    # Sort by category, then by rows_updated_dt descending if you prefer most recent first
    # or ascending if you want oldest first. Adjust accordingly. 
    # Here, let's assume ascending by date to mimic "then by updated date" in ascending order:
    datasets.sort(
        key=lambda x: (
            x['category'],
            x['rows_updated_dt'] if x['rows_updated_dt'] else datetime.min
        )
    )

    logger.info(f"Total datasets loaded and sorted: {len(datasets)}")
    return datasets


@app.get("/backend")
async def backend(request: Request):
    logger.info("Received request for /backend endpoint")
    datasets_info = load_and_sort_json()
    logger.debug(f"Loaded {len(datasets_info)} datasets")

    # For each dataset, find its output files
    for dataset in datasets_info:
        endpoint = dataset['endpoint']
        # Find matching output files
        output_files = find_output_files_for_endpoint(endpoint, output_dir)

        # Build URLs
        links = {}
        for file_type, path in output_files.items():
            links[file_type] = get_output_file_url(path, output_dir)
        dataset['output_links'] = links

    logger.info("Rendering 'backend.html' template with dataset information")
    return templates.TemplateResponse('backend.html', {"request": request, "datasets": datasets_info})


@app.get("/prep-data/{filename}")
async def prep_data(filename: str):
    logger.info(f"Received request to prep data for file: '{filename}'")
    current_dir = os.path.dirname(__file__)
    datasets_folder = os.path.join(current_dir, 'data', 'datasets')
    output_folder = os.path.join(current_dir, 'data', 'analysis_map')
    error_log = []

    # Check if file exists
    file_path = os.path.join(datasets_folder, filename)
    if not os.path.exists(file_path):
        logger.error(f"File '{filename}' not found in datasets directory.")
        raise HTTPException(status_code=404, detail=f"File '{filename}' not found in datasets directory.")

    try:
        # Process data
        process_single_file(filename, datasets_folder, output_folder, datetime.now(pytz.UTC), error_log)
        # Rebuild analysis map
        analysis_map_path = create_analysis_map(datasets_folder, output_folder)

        if error_log:
            logger.warning(f"Data prepared with warnings for file '{filename}': {error_log}")
            return JSONResponse({
                'status': 'warning',
                'message': 'Data prepared with warnings and analysis map updated.',
                'analysis_map': analysis_map_path,
                'errors': error_log
            })

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
        export_for_endpoint(endpoint, output_folder=output_dir,
                            log_file_path=os.path.join(output_dir, 'processing_log.txt'))
        if error_log:
            logger.warning(f"Analysis completed with warnings for endpoint '{endpoint}': {error_log}")
            return JSONResponse({'status': 'warning', 'message': 'Analysis completed with warnings.', 'errors': error_log})

        logger.info(f"Analysis for endpoint '{endpoint}' completed successfully.")
        return JSONResponse({'status': 'success', 'message': f'Analysis for endpoint {endpoint} completed successfully.'})
    except Exception as e:
        logger.exception(f"Error running analysis for endpoint '{endpoint}': {str(e)}")
        return JSONResponse({'status': 'error', 'message': str(e)})


@app.get("/get-updated-links/{endpoint}")
async def get_updated_links(endpoint: str):
    """
    Returns the updated output links for a single endpoint. Helps the front-end
    refresh the row's output links after an operation.
    """
    output_files = find_output_files_for_endpoint(endpoint, output_dir)
    links = {}
    for file_type, path in output_files.items():
        links[file_type] = get_output_file_url(path, output_dir)
    return JSONResponse({'links': links})


if __name__ == '__main__':
    import uvicorn
    logger.info("Starting FastAPI application")
    uvicorn.run(app, host="0.0.0.0", port=8000)
