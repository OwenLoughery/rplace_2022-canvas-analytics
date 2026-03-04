import duckdb
import pandas as pd

parquet_path = r"place_preprocessed.parquet"
sampled_path = r"place_sampled_timebins.parquet"

n_bins = 24
n_total = 720_000
per_bin = n_total // n_bins

con = duckdb.connect()

con.execute(f"""
    COPY (
      WITH bounds AS (
        SELECT
          min(ts) AS tmin,
          max(ts) AS tmax
        FROM read_parquet('{parquet_path}')
        WHERE ts IS NOT NULL
      ),
      base AS (
        SELECT
          e.ts, e.user_id, e.pc, e.x, e.y,
          least(
            {n_bins} - 1,
            greatest(
              0,
              floor(
                ({n_bins} * (epoch(e.ts) - epoch(b.tmin)))
                / nullif(epoch(b.tmax) - epoch(b.tmin), 0)
              )::INT
            )
          ) AS time_bin
        FROM read_parquet('{parquet_path}') e
        CROSS JOIN bounds b
        WHERE e.ts IS NOT NULL
      ),
      ranked AS (
        SELECT
          ts, user_id, pc, x, y,
          row_number() OVER (PARTITION BY time_bin ORDER BY random()) AS r
        FROM base
      )
      SELECT ts, user_id, pc, x, y
      FROM ranked
      WHERE r <= {per_bin}
    )
    TO '{sampled_path}'
    (FORMAT PARQUET, COMPRESSION ZSTD);
""")