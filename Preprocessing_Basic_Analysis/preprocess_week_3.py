import duckdb

csv_path = r"2022_place_canvas_history.csv"
parquet_path = r"place_preprocessed.parquet"

con = duckdb.connect()

con.execute(f"""
COPY (
    SELECT
        timestamp as ts,
        hash(user_id) as user_id,
        pixel_color as pc,
        CAST(split_part(coordinate, ',', 1) AS INTEGER) AS x,
        CAST(split_part(coordinate, ',', 2) AS INTEGER) AS y
    FROM read_csv_auto('{csv_path}', header=true)
)
TO '{parquet_path}'
(FORMAT PARQUET, COMPRESSION ZSTD);
""")
