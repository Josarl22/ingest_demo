````markdown
**Overview**
- **Purpose**: Minimal ingestion demo for local development and testing.
- **Entry point**: [ingest_demo/src/ingest_demo/main.py](ingest_demo/src/ingest_demo/main.py)

**Prerequisites**
- **Python**: 3.8+ installed and on PATH.
- **Poetry**: optional but recommended for dependency management.

- **ODBC driver**: If you plan to use `pyodbc`, install "ODBC Driver 17 for SQL Server" (msodbcsql17) or an equivalent driver from Microsoft's download page: https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server

**Quick Setup (PowerShell)**
- **Create workspace, venv, and activate**
```powershell
mkdir ingest_app
cd ingest_app
python -m venv .venv
.venv\Scripts\Activate.ps1
```
- **Upgrade packaging tools and install Poetry + helpers**
```powershell
python -m pip install --upgrade pip setuptools wheel poetry requests typing_extensions
```

- **Add runtime dependencies with Poetry**
```powershell
poetry add python-dotenv logging datetime pathlib pandas sqlalchemy pyodbc cryptography
```

**Environment variables**
- Create a `.env` file in the project root with your runtime config. Example:
```text
# .env
CONNECTION_STRING=Driver={ODBC Driver 17 for SQL Server};Server=tcp:yourhost.database.windows.net,1433;Database=mydb;UID=user;PWD=pass
CSV_COUNT=15
BATCH_SIZE=5
IS_CSV_RELOAD=True
MAX_WORKERS=4
CSV_URI="data/mock_transactions.csv"
AES_KEY= EXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXA=
```
**Install**
- **Install via Poetry**
```powershell
poetry install

**Run**
- **Run via Poetry (recommended)**
```powershell
poetry run python src/ingest_demo/main.py
```
- **Or run with the venv Python (from `ingest_demo` folder)**
```powershell
.venv\Scripts\Activate.ps1
python src/ingest_demo/main.py
```

**Project Structure (quick)**
- **Project root**: [ingest_demo](ingest_demo)
- **Source**: [ingest_demo/src/ingest_demo](ingest_demo/src/ingest_demo)
- **Main**: [ingest_demo/src/ingest_demo/main.py](ingest_demo/src/ingest_demo/main.py)

**Contributing**
- **Coding style**: follow existing project conventions.
- **Improvements**: create tests, and update this README if you add features.
````