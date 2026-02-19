import json
import requests
import logging
from pathlib import Path
from pydantic import ValidationError

#SRC Functions
from src.utils.file_ops import create_folder
from src.models import BreweryModel


logger = logging.getLogger(__name__)

def save_raw_page(path, page, n_page):
    """
    Save the raw data
    """
    file_path = path /  f'brewerys_page{n_page}.json'
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(page, f, ensure_ascii=False, indent=4)
    logger.info(f'[Bronze Ingestion] Data successfully persisted to: {file_path}')


def get_endpoint(base_url, endpoint, params = None):
    """
    General function to the breweries api
    """
    headers = {
        'Accept': 'application/json'
    }

    try:
        response = requests.get(f"{base_url}/{endpoint}", params=params, headers=headers, timeout=30)
        response.encoding = 'utf-8'
        response.raise_for_status() 
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f'[Bronze Ingestion] Critical error during extraction: {e}')
        raise


def fetch_raw_breweries(base_url, path):
    """
    Function that extract all the breweries from breweries list api
    """
    path = Path(path)
    params = {'page': 1, 'per_page': 200}
    path = create_folder(path=path, folder="data/bronze")

    while True:
        logger.info(f'[Bronze Ingestion] Fetching page {params["page"]} from API')
        page = get_endpoint(base_url, "breweries", params=params)

        if not page:
            logger.warning(f'[Bronze Ingestion] Page {params["page"]} returned no data')
            break

        errors_in_page = 0
        for item in page:
            try:
                BreweryModel(**item)
            except ValidationError as e:
                logger.warning(f'[Bronze Ingestion] Data Quality Alert | Page: {params["page"]} | ID: {item.get("id")} | Error: {e.json()}')
                errors_in_page += 1

            if '\ufffd' in str(item.values()):
                logger.warning(f'[Bronze Ingestion] Source Data Warning | Corrupted encoding detected (U+FFFD) | Page: {params["page"]} | ID: {item.get("id")}')

        
        save_raw_page(path, page=page, n_page=params['page'])
        
        if errors_in_page > 0:
            logger.warning(f'[Bronze Ingestion] Page {params["page"]} saved | {errors_in_page} inconsistent records detected')
        
        params["page"] += 1