import duckdb

SOURCE = "../../data/analyse/futures.trade.v1/*.parquet"

type_data = duckdb.sql(f"""
Describe Select * 
From read_parquet('{SOURCE}')""").show()

first_rows = duckdb.sql(f"""
Select * from read_parquet('{SOURCE}') limit 10""").show()

compression_types = duckdb.sql(f"""
Select distinct compression from parquet_metadata('{SOURCE}')""").show()

