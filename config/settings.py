import os

# MongoDB Configurations
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "midterm_bigdata_db")

COLLECTION_RAW = "orders_raw"
COLLECTION_VALIDATED = "orders_validated"
COLLECTION_QUARANTINE = "orders_quarantine"

SMALL_FILE_THRESHOLD_MB = float(os.getenv("SMALL_FILE_THRESHOLD_MB", 200.0))

# Batch Configurations
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 5000))
ELT_CHUNK_SIZE = int(os.getenv("ELT_CHUNK_SIZE", 5000))