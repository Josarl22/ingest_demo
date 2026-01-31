SALES_SCHEMA = {
    "table_name": "sales",
    "columns": [
        {"name": "transaction_id", "type": "Integer", "primary_key": True, "autoincrement": False},
        {"name": "customer_id", "type": "String", "length": 512},
        {"name": "product_id", "type": "Integer"},
        {"name": "quantity", "type": "Integer"},
        {"name": "sale_date", "type": "DateTime", "csv_name": "timestamp"},
    ]
}