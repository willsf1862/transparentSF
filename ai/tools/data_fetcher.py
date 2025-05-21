# data_fetcher.py
import requests
import re
import os
from urllib.parse import urljoin
import pandas as pd
import logging
import json

# Create a logger for this module
logger = logging.getLogger(__name__)
# Add these lines after the imports
logging.basicConfig(
    level=logging.INFO,  # Changed from DEBUG to INFO
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # This will output to console
    ]
)

def clean_query_string(query):
    """
    Cleans the query string by removing unnecessary whitespace and line breaks.
    """
    if not isinstance(query, str):
        logger.error(f"Query is not a string. Type: {type(query)}, Value: {query}")
        return str(query)
    cleaned = re.sub(r'\s+', ' ', query.replace('\n', ' ')).strip()
    logger.debug("Cleaned query string: %s", cleaned)
    return cleaned

def fetch_data_from_api(query_object):
    logger.info("Starting fetch_data_from_api with query_object: %s", json.dumps(query_object, indent=2))
    base_url = "https://data.sfgov.org/resource/"
    all_data = []
    limit = 5000
    offset = 0

    endpoint = query_object.get('endpoint')
    query = query_object.get('query')
    
    if not endpoint:
        logger.error("Missing endpoint in query_object")
        return {'error': 'Endpoint is required'}
    if not query:
        logger.error("Missing query in query_object")
        return {'error': 'Query is required'}

    logger.info(f"Processing endpoint: {endpoint}")
    logger.info(f"Initial query string: {query}")
    
    cleaned_query = clean_query_string(query)
    logger.info(f"After clean_query_string: {cleaned_query}")
    
    # Remove any $query= prefix if it exists
    if cleaned_query.startswith('$query='):
        cleaned_query = cleaned_query[7:]
        logger.info("Removed $query= prefix")
    if cleaned_query.startswith('query='):
        cleaned_query = cleaned_query[6:]
        logger.info("Removed query= prefix")
    
    logger.info(f"Final cleaned query: {cleaned_query}")

    has_limit = "limit" in cleaned_query.lower()
    url = urljoin(base_url, f"{endpoint if endpoint.endswith('.json') else endpoint + '.json'}")
    logger.info(f"Full API URL: {url}")
    
    # Don't wrap the query in $query= here, just pass it directly
    params = {"$query": cleaned_query}
    logger.info(f"Request parameters: {json.dumps(params, indent=2)}")

    headers = {
        'Accept': 'application/json'
    }
    logger.info(f"Request headers: {json.dumps(headers, indent=2)}")

    has_more_data = True
    while has_more_data:
        if not has_limit:
            paginated_query = f"{cleaned_query} LIMIT {limit} OFFSET {offset}"
            params["$query"] = paginated_query
        logger.debug("URL being requested: %s, params: %s", url, params)
        try:
            response = requests.get(url, params=params, headers=headers)
            logger.debug("Response Status Code: %s", response.status_code)
            response.raise_for_status()
            try:
                data = response.json()
            except ValueError:
                logger.exception(
                    "Failed to decode JSON response. Status Code: %s, Response Content: %s",
                    response.status_code,
                    response.text[:200]
                )
                return {'error': 'Failed to decode JSON response from the API.', 'queryURL': response.url}
            all_data.extend(data)
            logger.info("Fetched %d records in current batch.", len(data))

            if has_limit or len(data) < limit:
                has_more_data = False
                logger.debug("No more data to fetch; ending pagination.")
                logger.info("url: %s", url)
            else:
                offset += limit
                logger.debug("Proceeding to next offset: %d", offset)
        except requests.HTTPError as http_err:
            error_content = ''
            try:
                # Attempt to extract the error message from the response JSON
                error_json = response.json()
                error_content = error_json.get('message', response.text[:200])
            except ValueError:
                # If response is not JSON, use the text content
                error_content = response.text[:200]
            logger.exception(
                "HTTP error occurred: %s. Response Content: %s",
                http_err,
                error_content
            )
            return {'error': error_content, 'queryURL': response.url}
        except Exception as err:
            logger.exception("An error occurred: %s", err)
            return {'error': str(err), 'queryURL': response.url if response else None}

    logger.debug("Finished fetching data. Total records retrieved: %d", len(all_data))
    return {
        'data': all_data,
        'queryURL': response.url if response else None
    }

def fetch_facebook_ads_library(params):
    """Fetch data from the Facebook Ad Library API.

    Parameters
    ----------
    params : dict
        Dictionary of query parameters to pass directly to the API.

    Returns
    -------
    dict
        Dictionary containing either the returned data and query URL or an
        error message.
    """
    logger.info(
        "Starting fetch_facebook_ads_library with params: %s",
        json.dumps(params, indent=2)
    )
    base_url = "https://graph.facebook.com/v18.0/ads_archive"

    try:
        response = requests.get(base_url, params=params)
        logger.debug("Facebook API URL: %s", response.url)
        response.raise_for_status()
        try:
            data = response.json()
        except ValueError:
            logger.exception(
                "Failed to decode JSON response. Status Code: %s, Response Content: %s",
                response.status_code,
                response.text[:200],
            )
            return {
                'error': 'Failed to decode JSON response from Facebook API.',
                'queryURL': response.url,
            }

        record_count = len(data.get('data', [])) if isinstance(data, dict) else len(data)
        logger.info("Fetched %d records from Facebook API", record_count)
        return {
            'data': data,
            'queryURL': response.url,
        }
    except requests.HTTPError as http_err:
        error_content = ''
        try:
            error_json = response.json()
            if isinstance(error_json, dict):
                error_content = error_json.get('error', {}).get('message', response.text[:200])
            else:
                error_content = response.text[:200]
        except ValueError:
            error_content = response.text[:200]
        logger.exception(
            "HTTP error occurred: %s. Response Content: %s",
            http_err,
            error_content,
        )
        return {'error': error_content, 'queryURL': response.url}
    except Exception as err:
        logger.exception("An error occurred: %s", err)
        return {'error': str(err), 'queryURL': response.url if 'response' in locals() else None}

def fetch_facebook_ads_library_paged(params):
    """Fetch data from the Facebook Ad Library API with paging support.

    Parameters
    ----------
    params : dict
        Dictionary of query parameters to pass to the API. The dictionary may
        optionally contain an ``access_token`` key. If omitted, the function
        will attempt to read ``FACEBOOK_ACCESS_TOKEN`` from the environment.

    Returns
    -------
    dict
        Dictionary containing either the aggregated returned data and query
        URL or an error message.
    """
    logger.info(
        "Starting fetch_facebook_ads_library_paged with params: %s",
        json.dumps(params, indent=2),
    )

    base_url = "https://graph.facebook.com/v18.0/ads_archive"
    params = dict(params or {})
    access_token = params.get("access_token") or os.getenv("FACEBOOK_ACCESS_TOKEN")

    if not access_token:
        logger.error("Missing Facebook access token")
        return {"error": "Facebook access token is required", "queryURL": None}

    params["access_token"] = access_token

    all_data = []
    url = base_url

    while True:
        logger.debug("Facebook API request URL: %s params: %s", url, params)
        try:
            response = requests.get(url, params=params)
            logger.debug("Response Status Code: %s", response.status_code)
            response.raise_for_status()
            try:
                data = response.json()
            except ValueError:
                logger.exception(
                    "Failed to decode JSON response. Status Code: %s, Response Content: %s",
                    response.status_code,
                    response.text[:200],
                )
                return {
                    "error": "Failed to decode JSON response from Facebook API.",
                    "queryURL": response.url,
                }

            if not isinstance(data, dict):
                logger.error("Unexpected response format from Facebook API")
                return {
                    "error": "Unexpected response format from Facebook API",
                    "queryURL": response.url,
                }

            batch = data.get("data", [])
            all_data.extend(batch)
            logger.info("Fetched %d records in current batch.", len(batch))

            next_url = data.get("paging", {}).get("next")
            if not next_url:
                break
            url = next_url
            params = {}
        except requests.HTTPError as http_err:
            error_content = ""
            try:
                error_json = response.json()
                if isinstance(error_json, dict):
                    error_content = error_json.get("error", {}).get(
                        "message", response.text[:200]
                    )
                else:
                    error_content = response.text[:200]
            except ValueError:
                error_content = response.text[:200]
            logger.exception(
                "HTTP error occurred: %s. Response Content: %s",
                http_err,
                error_content,
            )
            return {"error": error_content, "queryURL": response.url}
        except Exception as err:
            logger.exception("An error occurred: %s", err)
            return {
                "error": str(err),
                "queryURL": response.url if "response" in locals() else None,
            }

    logger.info(
        "Finished fetching Facebook data. Total records retrieved: %d", len(all_data)
    )
    return {
        "data": all_data,
        "queryURL": response.url if "response" in locals() else None,
    }

def set_dataset(context_variables, *args, **kwargs):
    """
    Fetches data from the API and sets it in the context variables.
    
    Args:
        context_variables: Dictionary to store the dataset
        endpoint: The dataset identifier (e.g., 'ubvf-ztfx')
        query: The complete SoQL query string
        
    The function can be called in two ways:
    1. With positional arguments:
       set_dataset(context_variables, "dataset-id", query="your-soql-query")
    2. With keyword arguments:
       set_dataset(context_variables, endpoint="dataset-id", query="your-soql-query")
    3. With nested kwargs (agent style):
       set_dataset(context_variables, args="{}", kwargs={"endpoint": "x", "query": "y"})
        
    Returns:
        Dictionary with status and optional error message
    """
    logger.info("=== Starting set_dataset ===")
    logger.info(f"Args received: {args}")
    logger.info(f"Kwargs received: {json.dumps(kwargs, indent=2)}")
    logger.info(f"Context variables keys: {list(context_variables.keys())}")

    try:
        # Handle nested kwargs structure (agent style)
        if 'kwargs' in kwargs and isinstance(kwargs['kwargs'], dict):
            inner_kwargs = kwargs['kwargs']
            endpoint = inner_kwargs.get('endpoint')
            query = inner_kwargs.get('query')
        else:
            # Handle direct kwargs or positional args
            endpoint = args[0] if args else kwargs.get('endpoint')
            query = kwargs.get('query')
        
        # Validate required parameters
        if not endpoint:
            logger.error("Missing endpoint parameter")
            return {'error': 'Endpoint is required', 'queryURL': None}
        if not query:
            logger.error("Missing query parameter")
            return {'error': 'Query is required', 'queryURL': None}
            
        # Clean up endpoint - ensure it ends with .json
        if not endpoint.endswith('.json'):
            endpoint = f"{endpoint}.json"
            logger.info(f"Added .json to endpoint: {endpoint}")

        logger.info(f"Final parameters - Endpoint: {endpoint}, Query: {query}")
        query_object = {'endpoint': endpoint, 'query': query}
        
        result = fetch_data_from_api(query_object)
        logger.info(f"API result status: {'success' if 'data' in result else 'error'}")
        
        if result and 'data' in result:
            data = result['data']
            if data:
                df = pd.DataFrame(data)
                context_variables['dataset'] = df
                logger.info(f"Dataset successfully created with shape: {df.shape}")
                return {'status': 'success', 'queryURL': result.get('queryURL')}
            else:
                logger.warning("API returned empty data")
                return {'error': 'No data returned from the API', 'queryURL': result.get('queryURL')}
        elif 'error' in result:
            logger.error(f"API returned error: {result['error']}")
            return {'error': result['error'], 'queryURL': result.get('queryURL')}
        else:
            logger.error("Unexpected API response format")
            return {'error': 'Unexpected API response format', 'queryURL': result.get('queryURL')}
            
    except Exception as e:
        logger.exception("Unexpected error in set_dataset")
        return {'error': f'Unexpected error: {str(e)}', 'queryURL': None}
