import duckdb

full_path = r"place_preprocessed.parquet"
sampled_path = r"place_sampled_timebins.parquet"
labeled_path = r"place_sampled_labeled.parquet"
features_path = r"place_sampled_features.parquet"
train_path = r"train.parquet"
test_path  = r"test.parquet"

SURVIVE_MIN = 30
GRID = 20
LOOKBACK_MIN = 10

con = duckdb.connect()

# creating the feature we are trying to predict in the ML model which is whether the pixel survives
# in a set x amount of minutes after it was placed
con.execute(f"""
COPY (
    WITH full_next AS (
        SELECT
            ts, x, y,
            lead(ts) OVER (PARTITION BY x, y ORDER BY ts) AS next_ts
        FROM read_parquet('{full_path}')
    ),
    samp AS (
        SELECT ts, user_id, pc, x, y
        FROM read_parquet('{sampled_path}')
    )
    SELECT
        s.*,
        f.next_ts,
        CASE
            WHEN f.next_ts IS NULL THEN 1
            WHEN (epoch(f.next_ts) - epoch(s.ts))/60.0 >= {SURVIVE_MIN} THEN 1
            ELSE 0
        END AS y_survives_{SURVIVE_MIN}m
    FROM 
        samp AS s
    JOIN
        full_next AS f
        ON s.ts = f.ts AND s.x = f.x AND s.y = f.y
)
TO '{labeled_path}'
(FORMAT PARQUET, COMPRESSION ZSTD);
""")







con.execute(f"""
COPY (
    
    -- load labeled sample df and assign coords to grid cells
    WITH samp AS(
        SELECT 
            ts, user_id, pc, x, y, y_survives_{SURVIVE_MIN}m,
            floor(x / {GRID})::INT AS gx,
            floor(y / {GRID})::INT AS gy
        FROM read_parquet('{labeled_path}')
    ),
    
    -- gets dataset start time
    bounds AS (
        SELECT min(ts) AS tmin
        FROM read_parquet('{full_path}')
    ),
    
    -- computes pixel level history features from the full data set
    -- gets prior changes count & time since last change at that exact coord
    pixel_hist AS (
        SELECT
            ts, x, y,
            row_number() OVER (PARTITION BY x, y ORDER BY ts) - 1 AS prior_changes_at_pixel,
            epoch(ts) - epoch(lag(ts) OVER (PARTITION BY x, y ORDER BY ts)) AS time_since_last_change_sec
        FROM read_parquet('{full_path}')
    ),
    
    -- joins the pixel history feature onto the sampled rows df
    samp_hist AS (
        SELECT
            s.*,
            p.prior_changes_at_pixel,
            coalesce(p.time_since_last_change_sec, 1e9) AS time_since_last_change_sec
        FROM samp AS s
        JOIN pixel_hist AS p
            ON p.x = s.x AND p.y = s.y AND p.ts = s.ts
    ),
    
    -- creates the grid map of coords for the whole dataset
    full_grid AS (
        SELECT
            ts,
            floor(x / {GRID})::INT AS gx,
            floor(y / {GRID})::INT AS gy,
            user_id,
            pc
        FROM read_parquet('{full_path}')
    ),
    
    -- finds what was happening around each coord from sample in last chosen x amount of minutes before pixel was placed
    neighb AS (
        SELECT
            s.ts, s.x, s.y,
            count(*) AS local_event_count_{LOOKBACK_MIN}m,
            count(DISTINCT f.user_id) AS local_unique_users_{LOOKBACK_MIN}m,
            count(DISTINCT f.pc) AS local_unique_colors_{LOOKBACK_MIN}m
        FROM samp_hist s
        JOIN full_grid f
            ON f.gx = s.gx AND f.gy = s.gy
            AND f.ts >= s.ts - INTERVAL '{LOOKBACK_MIN} minutes'
            AND f.ts < s.ts
        GROUP BY s.ts, s.x, s.y
    )
    
    -- final select of all the features engineered to make the ML df
    SELECT
        s.ts, s.user_id, s.pc, s.x, s.y,
        s.y_survives_{SURVIVE_MIN}m,
        (epoch(s.ts) - epoch(b.tmin)) / 60.0 AS minutes_since_start,
        s.prior_changes_at_pixel,
        s.time_since_last_change_sec,
        coalesce(n.local_event_count_{LOOKBACK_MIN}m, 0) AS local_event_count_{LOOKBACK_MIN}m,
        coalesce(n.local_unique_users_{LOOKBACK_MIN}m, 0) AS local_unique_users_{LOOKBACK_MIN}m,
        coalesce(n.local_unique_colors_{LOOKBACK_MIN}m, 0) AS local_unique_colors_{LOOKBACK_MIN}m
    FROM samp_hist s
    CROSS JOIN bounds b 
    LEFT JOIN neighb n
        ON n.ts = s.ts AND n.x = s.x AND n.y = s.y
)
TO '{features_path}'
(FORMAT PARQUET, COMPRESSION ZSTD);
""")


# takes first 80% of sample ordered by the time to be the training dataset (doing an 80/20 train/test split)
con.execute(f"""
COPY ( 
    WITH ordered AS (
        SELECT *,
            ntile(5) OVER (ORDER BY ts) AS fold
        FROM read_parquet('{features_path}')
        )
    SELECT * EXCLUDE(fold)
    FROM ordered
    WHERE fold <= 4
)
TO '{train_path}' (FORMAT PARQUET, COMPRESSION ZSTD);
""")

# takes last 20% of sample ordered by the time to be the testing dataset (doing an 80/20 train/test split)
con.execute(f"""
COPY ( 
    WITH ordered AS (
        SELECT *,
            ntile(5) OVER (ORDER BY ts) AS fold
        FROM read_parquet('{features_path}')
        )
    SELECT * EXCLUDE(fold)
    FROM ordered
    WHERE fold = 5
)
TO '{test_path}' (FORMAT PARQUET, COMPRESSION ZSTD);
""")




