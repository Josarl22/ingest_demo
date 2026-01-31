# src/demo/main.py
from generic_methods import CSVBuilder, DataReader, DfSqlSyncer, EncryptionManager, LoggerDB, SqlAlchemyTypeMapper, Utils, DatabaseConnector, ParallelExecutor
from logger import get_logger
from config import EnvVariables, Paths, Queries
from constants import SALES_SCHEMA

# Configuration Variables
batch_size = EnvVariables().BATCH_SIZE
csv_path = Paths().CSV_PATH
csv_count = EnvVariables().CSV_COUNT
is_csv_reload = EnvVariables().IS_CSV_RELOAD
sql_connection_string = EnvVariables().SQL_CONNECTION_STRING
latest_sale_query = Queries().SELECT_SALES_LATEST_QRY
truncate_sales_query = Queries().TRUNCATE_SALES_TABLE_QRY
max_workers = EnvVariables().MAX_WORKERS
encryption_key = EnvVariables().AES_KEY  # 32 bytes key for AES-256
sales_schema = SALES_SCHEMA

logger = get_logger(__name__)

class ETL:
    def __init__(
        self, 
        sql_connection_string: str, 
        schema: dict,
        batch_size: int,
        csv_path: str,
        row_count: int,
        is_csv_reload: bool,
        truncate_query: str,
        latest_record_query: str,
    ):
        self.sql_connection_string = sql_connection_string
        self.schema = schema
        self.batch_size = batch_size
        self.csv_path = csv_path
        self.row_count = row_count
        self.is_csv_reload = is_csv_reload
        self.truncate_query = truncate_query
        self.latest_record_query = latest_record_query

    def provision_tables(self):
        conn = DatabaseConnector(connection_string=self.sql_connection_string)
        table_names = conn.get_table_names()
        if self.schema["table_name"] not in table_names:
            conn.provision_table_retry(schema=self.schema)
        if "logs" not in table_names:
            logger_db = LoggerDB(db_connector=conn, table_name="logs")
            logger_db.provision_log_table()

    def csv_builder(self):
        csv_builder = CSVBuilder(
            schema=self.schema, 
            batch_size=self.batch_size
        )
        csv_builder.build_csv(
            csv_path = self.csv_path, 
            row_count = self.row_count,
            is_reload = self.is_csv_reload
        )

    def extract(self):
        reader = DataReader()
        return reader.read_csv(path=self.csv_path)
    
    def transform(self, df):
        # Example transformation: Rename columns, handle missing values, encrypt sensitive data
        df.rename(columns={"timestamp": "sale_date"}, inplace=True)
        df = df.dropna(subset=['transaction_id'])
        df = df.dropna(how='all')
        df["customer_id"] = df["customer_id"].fillna(-9999)
        df["product_id"] = df["product_id"].fillna(-9999)
        df["quantity"] = df["quantity"].fillna(0)
        # Encrypt customer_id column
        aes = EncryptionManager(encryption_key)
        df["customer_id"] = df["customer_id"].apply(
            lambda x: aes.encrypt(str(x))
        )
        # Enforce Data Types as per Schema
        pd_schema = {
            col["name"]: SqlAlchemyTypeMapper.to_pandas(col)
            for col in sales_schema["columns"]
        }
        df = df.astype(pd_schema)
        # Sync DataFrame schema with SQL Table
        df = DfSqlSyncer().sync_df_mssql(
            df=df,
            connection_string=self.sql_connection_string,
            schema=self.schema,
        )
        return df
    
    def load(self, df):
        conn = DatabaseConnector(connection_string=self.sql_connection_string)
        # If reload is true, truncate the table, else get the latest sale_date
        if self.is_csv_reload:
            conn.query_ddl(self.truncate_query)
        else:
            latest_record = conn.query(self.latest_record_query)[0][0] or 0
            logger.info(f"Reading incremental data after: {latest_record}")
            df = df[df['sale_date'] > latest_record]
        # If no new data, exit
        if df.empty:
            msg = "No new data to load into SQL. Exiting."
            logger_db = LoggerDB(db_connector=conn, table_name="logs")
            logger_db.log_exception(msg)
            logger.warning(msg)
            exit(0)
        else:
            batches = [
                {
                    "rows": df.iloc[i:i+self.batch_size].to_dict(orient="records"),
                    "schema": self.schema,
                }
                for i in range(0, len(df), self.batch_size)
            ]
            ParallelExecutor(use_threads=True).gradual_caller(
                executor_func=conn.insert_data,
                grad_caller_args=batches,
                override_parallelism=max_workers,
            )
            logger.info("Data Ingestion Completed Successfully")

    def run(self):
        # CSV Generation
        self.csv_builder()
        # Provision Tables
        self.provision_tables()
        # Extracting Data from CSV 
        csv_df = self.extract()
        # Transformations are applied here
        transformed_df = self.transform(csv_df)
        # Load data into Database
        self.load(transformed_df)

# Main Ingestion Logic
if __name__ == "__main__":
    # logger = get_logger(__name__)
    logger.info("Ingestion Demo Started")
    csv_df = ETL(
        sql_connection_string=sql_connection_string,
        schema=sales_schema,
        batch_size=batch_size,
        csv_path=csv_path,
        row_count=csv_count,
        is_csv_reload=is_csv_reload,
        truncate_query=truncate_sales_query,
        latest_record_query=latest_sale_query,
    ).run()
    logger.info("Ingestion Demo Ended")