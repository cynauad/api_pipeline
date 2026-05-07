import requests
import logging
import time
from config import API_BASE_URL, user_name, API_TOKEN


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def fetch_data_with_retry(endpoint: str, max_retries=5, base_delay=1) -> dict:
    """Obtiene datos de la API, con reintentos en caso de error."""
    url = f"{API_BASE_URL}/{endpoint}.php?email={user_name}"
    params = {
        'key': API_TOKEN,
        'type': 'ecommerce',
        'rows': 1000
    }
    prepared = requests.Request('GET', url, params=params).prepare()
    logging.info(f"URL: {prepared.url}")

    logging.info(f"Fetching {params['rows']} rows of {params['type']} data...")
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()  # Lanza excepción si hay error HTTP
            logging.info(f"Status code: {response.status_code}")
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            if 400 <= response.status_code < 500:
                logging.error(f"Forbidden {response.status_code} - error del cliente. No se reintenta.")
                return None
            wait = base_delay * (2 ** attempt)  # Exponential backoff
            logging.warning(f"HTTP error: {http_err}. Retrying in {wait} seconds...")
            time.sleep(wait)
    logging.error("Se agotaron los reintentos")


if __name__ == "__main__":
    endpoint = "datasets"
    data = fetch_data_with_retry(endpoint)
    logging.info(f"Received {len(data.get('tables', {}).get('orders', []))} orders")
    if data:
        # print(type(data))
        # print(list(data.keys()))
        # print(type(data['tables']))
        # print(list(data['tables'].keys()))
        # print(type(data['tables']['orders']))
        print(data['tables']['orders'][:5])  # Muestra las primeras 5