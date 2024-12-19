from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
import os
import json
from datetime import datetime
from ai_dataprep import process_single_file  # Replace 'script_name' with the actual script filename
import pytz

app = FastAPI()
templates = Jinja2Templates(directory="templates")

def load_and_sort_json():
    datasets_dir = os.path.join(os.path.dirname(__file__), 'datasets')
    datasets = []

    for filename in os.listdir(datasets_dir):
        if filename.endswith('.json'):
            file_path = os.path.join(datasets_dir, filename)
            with open(file_path, 'r') as file:
                data = json.load(file)
                category = data.get('category', 'N/A')
                rows_updated_at = data.get('rows_updated_at', 'N/A')
                try:
                    if rows_updated_at != 'N/A':
                        rows_updated_at = datetime.strptime(rows_updated_at, '%Y-%m-%dT%H:%M:%SZ')
                except ValueError:
                    rows_updated_at = 'N/A'

                datasets.append({
                    'filename': filename,
                    'category': category,
                    'rows_updated_at': rows_updated_at
                })

    # Sort datasets by category, then by rows_updated_at
    datasets.sort(key=lambda x: (x['category'], x['rows_updated_at'] if x['rows_updated_at'] != 'N/A' else datetime.min))
    return datasets

@app.get("/backend")
async def backend(request: Request):
    datasets_info = load_and_sort_json()
    # Convert datetime back to string for template rendering
    for dataset in datasets_info:
        if dataset['rows_updated_at'] != 'N/A':
            dataset['rows_updated_at'] = dataset['rows_updated_at'].strftime('%Y-%m-%dT%H:%M:%SZ')
    return templates.TemplateResponse('backend.html', {"request": request, "datasets": datasets_info})

@app.get("/run_autorun/{filename}")
async def run_autorun(filename: str):
    datasets_folder = os.path.join(os.path.dirname(__file__), 'datasets')
    output_folder = os.path.join(os.path.dirname(__file__), 'analysis_map')
    threshold_date = datetime(2024, 9, 1, tzinfo=pytz.UTC)
    error_log = []

    # Check if the file exists
    file_path = os.path.join(datasets_folder, filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"File '{filename}' not found in datasets directory.")

    try:
        # Call the process_single_file function
        process_single_file(filename, datasets_folder, output_folder, threshold_date, error_log)

        # Return success response
        return JSONResponse({'status': 'success', 'message': f'File {filename} processed successfully.'})
    except Exception as e:
        return JSONResponse({'status': 'error', 'message': str(e)})

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
