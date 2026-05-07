from asyncio.log import logger

import requests
import logging
import time
import os
import pandas as pd
from config import API_BASE_URL, user_name, API_TOKEN


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def fetch_data_with_retry(endpoint: str, max_retries=5, base_delay=1) -> dict:
    """Obtiene datos de la API, con reintentos en caso de error."""
    
    logging.info("📥 Cargando datos...")
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


def transform_data(raw_data: dict) -> pd.DataFrame:
    """Transforma y mejora datos."""
    
    logging.info("🔄 Transformando data...")

    # extraccion de orders
    orders = raw_data.get('tables', {}).get('orders', [])
    df = pd.DataFrame(orders)

    if df.empty:
        logging.error("Datos no válidos para transformación")
        return pd.DataFrame()  # Devuelve un DataFrame vacío
    
    # conversiones
    # print(df.columns)
    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
    
    # enriquecimiento
    df['period'] = df['order_date'].dt.to_period('M').astype(str)  # Extraer periodo en formato "YYYY-MM"
    df['year'] = df['order_date'].dt.year.astype(str)  # Extraer año
    df['have_promotion'] = df['promotion_id'].notna().astype(int)  # 0 false, 1 true
    df = df.drop(columns=["promotion_id", "notes"]) # Eliminar columnas innecesarias

    # manejo de nulos
    total_invalid = df['total_amount'].isna().sum()
    if total_invalid > 0:
        logging.warning(f"Encontrados {total_invalid} registros con total_amount nulo. Se eliminarán.")
        df = df.dropna(subset=['total_amount'])  # Eliminar filas con total_amount nulo
    
    logging.info(f"Data transformed into DataFrame with {len(df)} rows and {len(df.columns)} columns")
    return df


def save_data(df: pd.DataFrame, output_dir: str = 'output'):
    """Guarda los resultados particionados por periodo."""
    logging.info(f"💾 Guardando datos en {output_dir}/...")

    # Crea carpeta output si no existe
    os.makedirs(output_dir, exist_ok=True)

    # Guardar datos
    df.to_parquet(
        f'{output_dir}/orders',
        partition_cols=['period'],
        index=False
    )

    df.to_parquet(f'{output_dir}/orders_full.parquet', index=False)

    logging.info("✅ Archivos Parquet guardados en output/")
    logging.info(f"Guardadas {len(df)} órdenes")
    logging.info(f"Particiones: {df['period'].nunique()} meses")


if __name__ == "__main__":
    endpoint = "datasets"
    
    logging.info("=" * 50)
    logging.info("API Pipeline - Iniciando")
    logging.info("=" * 50)
    
    try:

        data = fetch_data_with_retry(endpoint)
        logging.info(f"Received {len(data.get('tables', {}).get('orders', []))} orders")
        
        # if data:
        #     # print(type(data))
        #     # print(list(data.keys()))
        #     # print(type(data['tables']))
        #     # print(list(data['tables'].keys()))
        #     # print(type(data['tables']['orders']))
        #     print(data['tables']['orders'][:5])  # Muestra las primeras 5

        df = transform_data(data)
        # print(df.head())

        if df.empty:
            logging.error("No se generó ningún DataFrame válido. Terminando pipeline.")
        
        save_data(df)

        logging.info("=" * 50)
        logging.info("Pipeline completado exitosamente!")
        logging.info("=" * 50)
    
    except Exception as e:
        logging.error(f"Error en pipeline: {e}")
        raise