import os
from dotenv import load_dotenv

load_dotenv()

API_TOKEN = os.getenv("API_Token")
API_BASE_URL = os.getenv("API_BASE_URL", 'https://iansaura.com/api')
user_name = os.getenv("email")

if not API_TOKEN:
    raise ValueError("API_Token is not set in the environment variables. Please set it in the .env file.")
