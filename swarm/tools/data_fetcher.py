# data_fetcher.py
import requests
import re
from urllib.parse import urljoin
import pandas as pd
import logging

# Create a logger for this module
logger = logging.getLogger(__name__)

def clean_query_string(query):
    """
    Cleans the query string by removing unnecessary whitespace and line breaks.
    """
    cleaned = re.sub(r'\s+', ' ', query.replace('\n', ' ')).strip()
    logger.debug("Cleaned query string: %s", cleaned)
    return cleaned

def fetch_data_from_api(query_object):
    logger.info("Starting fetch_data_from_api with query_object: %s", query_object)
    base_url = "https://data.sfgov.org/resource/"
    all_data = []
    limit = 5000
    offset = 0

    endpoint = query_object.get('endpoint')
    query = query_object.get('query')
    if not endpoint or not query:
        logger.error("Invalid query object: %s", query_object)
        return {'error': 'Invalid query object provided.'}

    cleaned_query = clean_query_string(query)

    has_limit = "limit" in cleaned_query.lower()
    url = urljoin(base_url, endpoint)
    params = {"$query": cleaned_query}

    headers = {
        'Accept': 'application/json'
        # Include 'X-App-Token' if you have an app token
        # 'X-App-Token': 'YOUR_APP_TOKEN'
    }

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
                return {'error': 'Failed to decode JSON response from the API.'}
            all_data.extend(data)
            logger.info("Fetched %d records in current batch.", len(data))

            if has_limit or len(data) < limit:
                has_more_data = False
                logger.debug("No more data to fetch; ending pagination.")
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
            return {'error': error_content}
        except Exception as err:
            logger.exception("An error occurred: %s", err)
            return {'error': str(err)}

    logger.debug("Finished fetching data. Total records retrieved: %d", len(all_data))
    return {
        'data': all_data,
        'queryURL': response.url if response else None
    }

def set_dataset(context_variables, *args, **kwargs):
    """
    Fetches data from the API and sets it in the context variables.
    """
    logger.debug("Starting set_dataset with kwargs: %s", kwargs)
    endpoint = kwargs.get('endpoint')
    query = kwargs.get('query')
    if not endpoint or not query:
        logger.error("Endpoint and query are required parameters.")
        return {'error': 'Endpoint and query are required parameters.'}

    query_object = {'endpoint': endpoint, 'query': query}
    result = fetch_data_from_api(query_object)
    if result and 'data' in result:
        data = result['data']
        if data:
            df = pd.DataFrame(data)
            context_variables['dataset'] = df
            logger.info("Dataset successfully set with %d records.", len(df))
            return {'status': 'success', 'queryURL': result.get('queryURL')}
        else:
            logger.warning("No data returned from the API.")
            return {'error': 'No data returned from the API.'}
    elif 'error' in result:
        logger.error("Failed to fetch data from the API: %s", result['error'])
        return {'error': result['error']}
    else:
        logger.error("Failed to fetch data from the API.")
        return {'error': 'Failed to fetch data from the API.'}
