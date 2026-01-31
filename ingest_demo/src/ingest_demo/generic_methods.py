import os
from logger import get_logger
import time
from dataclasses import dataclass, field
import datetime as dt
import csv
import random
import pandas as pd
from sqlalchemy.engine import URL
import sqlalchemy as sa
from typing import Callable, List, Dict, Optional, Union, Any
import concurrent.futures
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import base64

logger = get_logger(__name__)


@dataclass
class Transaction:
    transaction_id: int
    customer_id: str
    product_id: int
    quantity: int
    timestamp: dt.datetime = field(default_factory=dt.datetime.now)


class CSVBuilder:
    def __init__(self, schema: dict, batch_size: int = 10_000):
        self.schema = schema
        self.batch_size = batch_size

    def get_last_transaction_id(self, csv_path: str) -> int:
        """
        Get the last transaction ID from a CSV file.

        Parameters
        ----------
        csv_path : str
            Path to the CSV file.

        Returns
        -------
        int
            The last transaction ID found in the CSV file.
            Returns 0 if the file does not exist, is empty, or cannot be parsed.
        """
        if not os.path.exists(csv_path):
            return 0
        with open(csv_path, "r", encoding="utf-8") as f:
            try:
                last_line = None
                for last_line in f:
                    pass  # iterate to last line
                if last_line is None:
                    return 0
                return int(last_line.split(",")[0])
            except (ValueError, IndexError):
                return 0

    def generate_batch(
        self,
        start_id: int,
        batch_size: int,
    ) -> List[dict]:
        """
        Generate a batch of transaction records with optional nulls.

        Parameters
        ----------
        start_id : int
            The starting transaction ID for the batch.
        batch_size : int
            Number of rows to generate in this batch.

        Returns
        -------
        list of dict
            A list of dictionaries, each representing a transaction with keys:
            'transaction_id', 'customer_id', 'product_id', 'quantity', 'timestamp'.
            Some values may be None to simulate missing data.
        """
        rows = []
        now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        columns = self.schema["columns"]

        for i in range(start_id, start_id + batch_size):
            row = {}
            null_chance = random.random()

            for col in columns:
                col_name = col.get("csv_name", col["name"])
                col_type = col["type"].lower()
                is_pk = col.get("primary_key", False)

                if col_type in ("datetime", "timestamp"):
                    row[col_name] = now
                    continue

                # Global null row (except PK)
                if null_chance < 0.10 and not is_pk:
                    row[col_name] = None
                    continue

                # Partial nulls
                if null_chance < 0.20 and not is_pk and random.random() < 0.2:
                    row[col_name] = None
                    continue

                # Value generation by type
                if is_pk:
                    row[col_name] = i
                elif col_type in ("integer", "int", "bigint"):
                    row[col_name] = random.randint(1, 999)
                elif col_type in ("string", "varchar", "nvarchar"):
                    row[col_name] = str(random.randint(1, 999))
                else:
                    row[col_name] = None

            rows.append(row)

        return rows

    def build_csv(
        self, csv_path: str, row_count: int, is_reload: Optional[bool] = False
    ) -> None:
        """
        Build a CSV file with transaction data in batches.

        Parameters
        ----------
        csv_path : str
            Path to the CSV file to write to.
        row_count : int
            Total number of rows to generate.
        is_reload : bool, optional
            If True, overwrite the existing CSV file and reset transaction IDs.
            If False, append to the CSV and continue from the last transaction ID.

        Returns
        -------
        None
        """
        mode = "w" if is_reload else "a"
        columns = self.schema["columns"]

        # CSV headers (respect csv_name)
        headers = [col.get("csv_name", col["name"]) for col in columns]

        file_exists = os.path.exists(csv_path)

        # Determine starting ID
        if is_reload or not file_exists:
            current_id = 1
        else:
            current_id = self.get_last_transaction_id(csv_path) + 1

        remaining = row_count

        with open(csv_path, mode, newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)

            if is_reload or not file_exists:
                writer.writeheader()
            while remaining > 0:
                batch_len = min(self.batch_size, remaining)

                batch = self.generate_batch(
                    start_id=current_id,
                    batch_size=batch_len,
                )

                writer.writerows(batch)

                current_id += batch_len
                remaining -= batch_len

class DataReader:
    def __init__(self):
        pass

    def read_csv(self, path: str) -> pd.DataFrame:
        """
        Read a CSV file into a pandas DataFrame.

        Parameters
        ----------
        path : str
            Path to the CSV file to read.

        Returns
        -------
        pd.DataFrame
            DataFrame containing the CSV data.

        Raises
        ------
        FileNotFoundError
            If the CSV file does not exist at the specified path.
        Exception
            For any other errors encountered while reading the CSV file.
        """
        try:
            df = pd.read_csv(path)
            return df
        except FileNotFoundError:
            logger.error(f"CSV file not found: {path}")
            raise
        except Exception as e:
            logger.error(f"Error reading CSV: {e}")
            raise


class DatabaseConnector:
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.SQL_TYPE_MAP = {
            "Integer": sa.Integer,
            "String": sa.String,
            "DateTime": sa.DateTime,
        }

    def get_engine(self) -> sa.Engine:
        """
        Create and return a SQLAlchemy engine for the database connection.

        Returns
        -------
        sa.Engine
            SQLAlchemy engine object configured with fast_executemany.
        """
        connection_url = URL.create(
            "mssql+pyodbc", query={"odbc_connect": self.connection_string}
        )
        engine = sa.create_engine(connection_url, fast_executemany=True)
        return engine
    
    def get_table_names(self) -> List[str]:
        """
        Retrieve the list of table names in the connected database.

        Returns
        -------
        list of str
            List of table names present in the database.
        """
        engine = self.get_engine()
        inspector = sa.inspect(engine)
        return inspector.get_table_names()

    def create_connection(self):
        """
        """
        engine = self.get_engine()
        conn = engine.connect()
        return conn

    def build_table_from_schema(self, schema: dict, sql_type_map: dict, metadata=None) -> sa.Table:
        """
        Build a SQLAlchemy Table object from a schema dictionary.

        Parameters
        ----------
        schema : dict
            A dictionary defining the table schema. Must include:
            - "table_name": str, the name of the table.
            - "columns": list of dicts, each with:
                - "name": str, column name
                - "type": str, column type key matching `sql_type_map`
                - "length": int, optional, only for string columns
                - "primary_key": bool, optional, whether this column is a primary key
                - "autoincrement": bool, optional, whether this column should auto-increment (numeric types only)
        sql_type_map : dict
            Mapping from schema type strings to SQLAlchemy column types, e.g.:
            {"Integer": sa.Integer, "String": sa.String, "DateTime": sa.DateTime}
        metadata : sqlalchemy.MetaData, optional
            An optional SQLAlchemy MetaData object to register the table with.
            If None, a new MetaData object will be created.

        Returns
        -------
        sa.Table
            A SQLAlchemy Table object constructed according to the schema.

        Notes
        -----
        - Only columns with `primary_key=True` or `autoincrement=True` will have those
        attributes set; all other columns are created without those flags.
        - For SQL Server, `autoincrement=True` should only be used on integer-type columns.
        """
        metadata = metadata or sa.MetaData()
        columns = []
        for col in schema["columns"]:
            col_type = sql_type_map[col["type"]]
            if col["type"] == "String":
                col_type = col_type(col.get("length", 255))
            else:
                col_type = col_type()

            kwargs = {}
            if col.get("primary_key") is True:
                kwargs["primary_key"] = True
            if col.get("autoincrement") is True:
                kwargs["autoincrement"] = True

            columns.append(
                sa.Column(
                    col["name"],
                    col_type,
                    **kwargs
                )
            )
        return sa.Table(schema["table_name"], metadata, *columns)

    def provision_table(self, schema: dict) -> bool:
        """
        Provision a table in the database according to the provided schema.

        This method creates the table if it does not already exist,
        using the SQLAlchemy metadata system. The table structure
        is defined dynamically from the given schema dictionary.

        Parameters
        ----------
        schema : dict
            A dictionary describing the table to provision. Expected keys:
            - "table_name": str
                Name of the table to create.
            - "columns": list of dicts
                Each dictionary defines a column with keys:
                - "name": str, column name
                - "type": str, key matching `self.SQL_TYPE_MAP`
                - "length": int, optional, only for string columns
                - "primary_key": bool, optional
                - "autoincrement": bool, optional

        Returns
        -------
        bool
            True if the table was successfully created or already exists.

        Notes
        -----
        - Only columns with `primary_key=True` or `autoincrement=True` will
        have those attributes set.
        - For SQL Server, `autoincrement=True` should only be used on integer-type columns.
        """

        metadata = sa.MetaData()
        engine = self.get_engine()
        # Build the table from the provided schema
        table = self.build_table_from_schema(schema, self.SQL_TYPE_MAP, metadata)
        metadata.create_all(engine)
        return True

    def provision_table_retry(self, schema: dict, retries: int = 3, delay: int = 20) -> bool:
        """
        Retry provisioning a table in the database multiple times.

        Parameters
        ----------
        schema : dict
            Dictionary describing the table schema (same format as for `provision_sales`).
        retries : int, optional
            Number of retry attempts if provisioning fails (default is 3).
        delay : int, optional
            Delay in seconds between retry attempts (default is 10).

        Returns
        -------
        bool
            True if provisioning succeeded within the given retries; False otherwise.

        Notes
        -----
        - Relies on `Utils.retry` to handle retries and exception logging.
        - Errors will be logged with the message: "Failed to provision sales table".
        """

        return Utils.retry(
            self.provision_table,
            schema=schema,
            retries=retries,
            delay=delay,
            error_msg="Failed to provision table",
        )

    def insert_data(self, rows: List[Dict], schema: dict) -> bool:
        """
        Insert rows into the table defined by `schema`.

        Parameters
        ----------
        rows : list of dict
            List of rows to insert, each row being a dictionary with keys:
            'transaction_id', 'customer_id', 'product_id', 'quantity', 'sale_date'.
        schema : dict
            A dictionary defining the table schema (same format as for `provision_sales`).

        Returns
        -------
        bool
            True if rows were successfully inserted.
        """
        metadata = sa.MetaData()
        engine = self.get_engine()
        # Build the table from the provided schema
        table = self.build_table_from_schema(schema, self.SQL_TYPE_MAP, metadata)

        with engine.begin() as conn:
            if isinstance(rows, dict):
                rows = [rows]  # single row -> list
            conn.execute(table.insert(), rows)
        return True

    def query(self, query: str, **params):
        """
        Execute a SQL query with optional parameters.

        Parameters
        ----------
        query : str
            The SQL query to execute.
        **params : dict
            Optional query parameters for parameterized queries.

        Returns
        -------
        list or None
            List of result rows for SELECT queries.
            None for non-returning statements like INSERT/UPDATE/MERGE.
        """
        conn = self.create_connection()
        with conn as connection:
            result = connection.execute(sa.text(query), params)
            # Only fetch rows if the statement returns rows

            if result.returns_rows:
                return result.fetchall()
            return None  # For INSERT/UPDATE/MERGE statements

    def query_ddl(self, query: str) -> bool:
        """
        Execute a DDL statement (CREATE, ALTER, DROP) directly on the database.

        Parameters
        ----------
        query : str
            The DDL SQL statement to execute.

        Returns
        -------
        bool
            True if the DDL statement executed successfully.
        """
        engine = self.get_engine()

        # Use raw connection for full control

        conn = engine.raw_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()  # DDL needs commit in SQL Server
            cursor.close()
        finally:
            conn.close()
        return True


class ParallelExecutor:

    def __init__(self, use_threads: bool = True):
        self.use_threads = use_threads

    def gradual_caller(
        self,
        executor_func: Optional[Callable] = None,
        grad_caller_args: list = None,
        override_parallelism: int = 1,
        timeout: int = 7200,
    ):
        """
        Execute the given function concurrently over provided arguments.

        Parameters
        ----------
        executor_func : Callable
            The function to execute concurrently.
        grad_caller_args : list or dict
            If list: list of argument tuples for the function.
            If dict: key = identifier, value = argument tuple.
        override_parallelism : int
            Overrides max_workers. If -1, uses default max_workers.
        timeout : int
            Timeout for each future in seconds.

        Returns
        -------
        results : dict
            Dictionary mapping keys (or indices) to results or exceptions.
        """
        if executor_func is None:
            raise ValueError("executor_func must be provided")
        if grad_caller_args is None:
            raise ValueError("grad_caller_args must be provided")
        ExecutorClass = (
            concurrent.futures.ThreadPoolExecutor
            if self.use_threads
            else concurrent.futures.ProcessPoolExecutor
        )
        results = {}

        with ExecutorClass(max_workers=override_parallelism) as executor:
            future_func = {
                executor.submit(executor_func, **arg): arg for arg in grad_caller_args
            }
            for future in concurrent.futures.as_completed(future_func):
                arg = future_func[future]
                try:
                    data = future.result()
                except Exception as exc:
                    logger.error("%r generated an exception: %s" % (arg, exc))


class Utils:
    @staticmethod
    def rocket_countdown(start: int = 3) -> None:
        """
        Print a rocket-style countdown in the console.

        Parameters
        ----------
        start : int, optional
            The starting number for the countdown (default is 3).

        Returns
        -------
        None
        """
        logger.info("Application is going to start...")
        for i in range(start, 0, -1):
            logger.info(f"{i}...")
            time.sleep(1)
        logger.info("Lift-off! 🚀")

    @staticmethod
    def retry(
        fn: Callable[..., bool],
        *args: Any,
        retries: int = 2,
        delay: int = 10,
        error_msg: str = "Operation failed",
        **kwargs: Any,
    ) -> bool:
        """
        Retry a function multiple times with a delay.

        Parameters
        ----------
        fn : Callable[..., bool]
            The function to retry. Should return True on success.
        *args : Any
            Positional arguments passed to the function.
        retries : int, optional
            Number of retry attempts (default is 2).
        delay : int, optional
            Delay in seconds between retries (default is 10).
        error_msg : str, optional
            Error message to log on final failure (default is "Operation failed").
        **kwargs : Any
            Keyword arguments passed to the function.

        Returns
        -------
        bool
            True if the function succeeded within the given retries.
        """
        for attempt in range(retries):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                if attempt == retries - 1:
                    logger.error(f"{error_msg}: {e}")
                    raise
                time.sleep(delay)

class LoggerDB:

    def __init__(self, db_connector: DatabaseConnector, table_name: str = "logs"):
        self.db_connector = db_connector
        self.table_name = table_name

    def provision_log_table(self) -> bool:
        """
        Provision the logs table in the database if it does not exist.

        The table schema includes:
        - id (int, primary key, autoincrement)
        - log_time (datetime)
        - level (str)
        - message (str)

        Returns
        -------
        bool
            True if the table was successfully created or already exists.
        """
        metadata = sa.MetaData()
        engine = self.db_connector.get_engine()

        sa.Table(
            self.table_name,
            metadata,
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("log_time", sa.DateTime, default=dt.datetime.now),
            sa.Column("level", sa.String(20), nullable=False),
            sa.Column("message", sa.String, nullable=False),
        )

        metadata.create_all(engine)
        return True

    def log(self, level: str, message: str) -> bool:
        """
        Insert a log record into the logs table.

        Parameters
        ----------
        level : str
            Log level, e.g., "INFO", "ERROR", "DEBUG".
        message : str
            The log message.

        Returns
        -------
        bool
            True if the log was successfully persisted.
        """
        engine = self.db_connector.get_engine()
        metadata = sa.MetaData()
        logs_table = sa.Table(
            self.table_name,
            metadata,
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("log_time", sa.DateTime, default=dt.datetime.utcnow),
            sa.Column("level", sa.String(20), nullable=False),
            sa.Column("message", sa.String, nullable=False),
        )

        metadata.create_all(engine)

        with engine.begin() as conn:
            conn.execute(
                logs_table.insert(),
                {
                    "log_time": dt.datetime.now(),
                    "level": level,
                    "message": message,
                },
            )
        return True

    def log_exception(self, exc: Exception) -> bool:
        """
        Convenience method to log exceptions.

        Parameters
        ----------
        exc : Exception
            The exception instance to log.

        Returns
        -------
        bool
            True if the exception log was successfully persisted.
        """
        return self.log(level="ERROR", message=str(exc))

class EncryptionManager:

    NONCE_SIZE = 12  # GCM standard

    def __init__(self, key: bytes):
        if len(key) != 32:
            raise ValueError("Key must be 32 bytes (AES-256)")
        self._aes = AESGCM(key)

    def encrypt(self, value: str) -> str:
        """
        Encrypt a string value using AES-256-GCM.

        Parameters
        ----------
        value : str
            The plaintext string to encrypt.

        Returns
        -------
        str
            Base64-encoded ciphertext including the nonce.

        Raises
        ------
        TypeError
            If `value` is not a string.
        """
        nonce = os.urandom(self.NONCE_SIZE)
        ciphertext = self._aes.encrypt(
            nonce,
            value.encode("utf-8"),
            None
        )
        return base64.b64encode(nonce + ciphertext).decode("utf-8")

    def decrypt(self, value: str) -> str:
        """
        Decrypt a base64-encoded AES-256-GCM ciphertext.

        Parameters
        ----------
        value : str
            Base64-encoded ciphertext that includes the nonce.

        Returns
        -------
        str
            The original decrypted plaintext string.

        Raises
        ------
        TypeError
            If `value` is not a string.
        ValueError
            If `value` cannot be base64-decoded or is invalid ciphertext.
        """
        raw = base64.b64decode(value)
        nonce = raw[:self.NONCE_SIZE]
        ciphertext = raw[self.NONCE_SIZE:]
        return self._aes.decrypt(
            nonce,
            ciphertext,
            None
        ).decode("utf-8")

class SqlAlchemyTypeMapper:
    """
    Maps SQLAlchemy-like column definitions to concrete target types.
    """

    MSSQL_MAP = {
        "integer": "BIGINT",
        "int": "BIGINT",
        "bigint": "BIGINT",
        "float": "FLOAT",
        "numeric": "FLOAT",
        "decimal": "FLOAT",
        "boolean": "BIT",
        "bool": "BIT",
        "datetime": "DATETIME2",
        "timestamp": "DATETIME2",
        "date": "DATE",
    }

    PANDAS_MAP = {
        "integer": "Int64",
        "int": "Int64",
        "bigint": "Int64",
        "float": "Float64",
        "numeric": "Float64",
        "decimal": "Float64",
        "boolean": "boolean",
        "bool": "boolean",
        "datetime": "datetime64[ns]",
        "timestamp": "datetime64[ns]",
        "date": "datetime64[ns]",
        "string": "string",
        "varchar": "string",
        "nvarchar": "string",
    }

    @classmethod
    def to_mssql(cls, col_def: dict) -> str:
        """
        Convert SQLAlchemy-like column definition to MSSQL type.
        """
        col_type = col_def["type"].lower()

        if col_type in ("string", "varchar", "nvarchar"):
            length = col_def.get("length", 255)
            return f"NVARCHAR({length})"

        return cls.MSSQL_MAP.get(col_type, "NVARCHAR(MAX)")

    @classmethod
    def to_pandas(cls, col_def: dict) -> str:
        """
        Convert SQLAlchemy-like column definition to Pandas dtype.
        """
        col_type = col_def["type"].lower()
        pandas_type = cls.PANDAS_MAP.get(col_type, "object")

        # Enforce non-nullability for primary keys
        if col_def.get("primary_key"):
            if pandas_type == "Int64":
                return "int64"
            if pandas_type == "string":
                return "object"

        return pandas_type
    
class DfSqlSyncer:
    def __init__(self, ):
        pass

    def sync_df_mssql(
            self, 
            df: pd.DataFrame, 
            connection_string: str, 
            schema: dict
        ) -> pd.DataFrame:
        """
        Sync a pandas DataFrame schema with a MSSQL table schema.
        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame to sync.
        connection_string : str
            The database connection string.
        schema : dict
            The table schema definition.
        """
        conn = DatabaseConnector(connection_string=connection_string)

        table_name=schema['table_name']
        df_columns = set(df.columns)

        engine = conn.get_engine()
        inspector = sa.inspect(engine)
        sql_columns = set([col['name'] for col in inspector.get_columns(table_name)])

        # Sync schemas
        for col in sql_columns - df_columns:
            df[col] = pd.NA
            logger.info(f"Altering DataFrame to add column: {col}")
        with engine.connect() as conn:
            for col in df_columns - sql_columns:
                # Determine SQL type from your enforced schema
                col_metadata = next((c for c in schema['columns'] if c['name'] == col), None)
                sql_type = SqlAlchemyTypeMapper.to_mssql(col_metadata)
                logger.info(f"Altering table to add column: {col} with type {sql_type}")
                conn = DatabaseConnector(connection_string=connection_string)
                conn.query_ddl(f'ALTER TABLE {table_name} ADD [{col}] {sql_type};')

        return df
