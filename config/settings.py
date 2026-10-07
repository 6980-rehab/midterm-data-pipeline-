from dotenv import load_dotenv
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "midterm_data_pipeline_bigdata")
COLLECTION_RAW = os.getenv("COLLECTION_RAW", "orders_raw")
COLLECTION_VALIDATED = os.getenv("COLLECTION_VALIDATED", "orders_validated")
COLLECTION_QUARANTINE = os.getenv("COLLECTION_QUARANTINE", "orders_quarantine")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "2000"))
ELT_CHUNK_SIZE = int(os.getenv("ELT_CHUNK_SIZE", "2000"))
SMALL_FILE_THRESHOLD_MB = int(os.getenv("SMALL_FILE_THRESHOLD_MB", "200"))
API_HOST = os.getenv("API_HOST", "127.0.0.1")
API_PORT = int(os.getenv("API_PORT", "8000"))
JOB_DAILY_SALES_HOUR = int(os.getenv("JOB_DAILY_SALES_HOUR", "23"))
JOB_TOP_PRODUCTS_HOUR = int(os.getenv("JOB_TOP_PRODUCTS_HOUR", "23"))
