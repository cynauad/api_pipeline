# API Pipeline — Ecommerce Orders

Pipeline de extracción, transformación y almacenamiento de datos de órdenes de ecommerce desde una API REST.

## Descripción

El pipeline consume datos de la API de [iansaura.com](https://iansaura.com/api), transforma las órdenes en un formato analítico y las guarda en archivos Parquet particionados por mes.

```
API REST → fetch_data_with_retry() → transform_data() → save_data() → Parquet
```

## Estructura del proyecto

```
api_pipeline/
├── main.py          # Pipeline principal (extracción, transformación, guardado)
├── config.py        # Configuración y variables de entorno
├── requirements.txt # Dependencias
├── .env             # Credenciales (no se versiona)
└── output/          # Archivos Parquet generados
    ├── orders/      # Particionado por periodo (YYYY-MM)
    └── orders_full.parquet
```

## Requisitos

- Python 3.8+
- Credenciales de acceso a la API (token y email)

## Instalación

```bash
# Crear entorno virtual
python -m venv venv-api

# Activar entorno virtual (bash)
source venv-api/Scripts/activate

# Instalar dependencias
pip install -r requirements.txt
```

## Configuración

Crear un archivo `.env` en la raíz del proyecto:

```
API_Token= TU_TOKEN_AQUI
username= TU@EMAIL
API_BASE_URL=https://iansaura.com/api
```

## Uso

```bash
python main.py
```

## Etapas del pipeline

### 1. Extracción (`fetch_data_with_retry`)
- Realiza un GET a la API con autenticación por token
- Implementa reintentos con exponential backoff para errores 5xx
- No reintenta errores 4xx (credenciales incorrectas, permisos, etc.)

### 2. Transformación (`transform_data`)
- Extrae la tabla `orders` del response
- Convierte `order_date` a tipo datetime
- Agrega columnas derivadas: `period` (YYYY-MM), `year`, `have_promotion`
- Elimina columnas innecesarias: `promotion_id`, `notes`

Decisiones de limpieza

| Campo | Problema | Decisión |
|---|---|---|
| `notes` | 85.5% nulos (texto libre sin valor estructural) | Eliminado |
| `promotion_id` | 75.4% nulos | Convertido a binario: `have_promotion` (0/1) |
| Fechas | Tipo object | Convertidas a datetime: `order_date`

### 3. Almacenamiento (`save_data`)
- Guarda un archivo Parquet completo: `output/orders_full.parquet`
- Guarda una versión particionada por mes: `output/orders/period=YYYY-MM/`

## Dependencias

| Librería | Uso |
|----------|-----|
| `requests` | Llamadas HTTP a la API |
| `pandas` | Transformación de datos |
| `pyarrow` | Escritura de archivos Parquet |
| `python-dotenv` | Lectura de variables de entorno desde `.env` |


## Autor
Cynthia Auad