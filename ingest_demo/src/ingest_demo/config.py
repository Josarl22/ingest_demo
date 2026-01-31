import os
from dotenv import load_dotenv
from pathlib import Path
import base64

load_dotenv()  # loads .env file into environment variables

class EnvVariables:
    CSV_COUNT: int = int(os.getenv("CSV_COUNT", "10000"))
    SQL_CONNECTION_STRING: str = os.getenv("CONNECTION_STRING", "")
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "1000"))
    IS_CSV_RELOAD: bool = os.getenv("IS_CSV_RELOAD", "False").lower() in ("true", "1")
    MAX_WORKERS: int = int(os.getenv("MAX_WORKERS", "4"))
    AES_KEY: bytes = base64.b64decode(os.getenv("AES_KEY", "")) 

class Paths:
    BASE_DIR = Path(__file__).parent.parent.parent  # points to project root
    CSV_URI = Path(os.getenv("CSV_URI", "data/mock_transactions.csv"))
    CSV_PATH = f"{BASE_DIR}/{CSV_URI}"

class Queries:
    SELECT_SALES_LATEST_QRY = "SELECT MAX(sale_date) FROM sales;"
    TRUNCATE_SALES_TABLE_QRY = "TRUNCATE TABLE sales;"

