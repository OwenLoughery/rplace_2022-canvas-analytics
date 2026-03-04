import duckdb
import pandas as pd
from plotnine import *
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)



con = duckdb.connect()




#######################
#######################
# BUCKET THREE
#######################
#######################

####################################################################################
# MASS FIRST TIME USER SPIKE SECTION EXPLORING IF POSSIBLE IRREGULAR ACTIVITY AREA
####################################################################################


df = con.execute("""
    SELECT DATE_TRUNC('minute', ts) AS minute,
           COUNT(*) AS placements
    FROM parquet_scan('place_preprocessed.parquet')
    GROUP BY minute
    ORDER BY minute
""").df()

g = (
    ggplot(df, aes(x="minute", y="placements"))
    + geom_line()
    + labs(title="r/place Activity Over Time",
           x="Time",
           y="Placements per Minute")
    + theme_minimal()
)
g.show()

df_top_counts = con.execute("""
    SELECT DATE_TRUNC('minute', ts) AS minute,
           COUNT(*) AS placements
    FROM parquet_scan('place_preprocessed.parquet')
    GROUP BY minute
    ORDER BY placements DESC
""").df()
print(df_top_counts.head(10))




df_high_placement_time = con.execute("""
    SELECT DATE_TRUNC('minute', ts) AS minute,
           COUNT(*) AS placements
    FROM parquet_scan('place_preprocessed.parquet')
    WHERE ts between '2022-04-04 21:30:00' AND '2022-04-04 23:10:00'
    GROUP BY minute
    ORDER BY minute
""").df()

g_window = (
    ggplot(df_high_placement_time, aes(x="minute", y="placements"))
    + geom_line()
    + labs(title="r/place Activity Over Time",
           x="Time",
           y="Placements per Minute")
    + theme_minimal()
)
#g_window.show()


df_high_placement_time_03 = con.execute("""
    SELECT DATE_TRUNC('minute', ts) AS minute,
           COUNT(*) AS placements
    FROM parquet_scan('place_preprocessed.parquet')
    WHERE ts between '2022-04-03 20:30:00' AND '2022-04-03 21:30:00'
    GROUP BY minute
    ORDER BY minute
""").df()

g_window_03 = (
    ggplot(df_high_placement_time_03, aes(x="minute", y="placements"))
    + geom_line()
    + labs(title="r/place Activity Over Time",
           x="Time",
           y="Placements per Minute")
    + theme_minimal()
)
g_window_03.show()


coords = (con.execute("""
    SELECT x, y, COUNT(*) as edits
    FROM parquet_scan('place_preprocessed.parquet')
    WHERE DATE_TRUNC('minute', ts) = '2022-04-03 21:00:00'
    GROUP BY x, y
""").df())

g_coord = (
    ggplot(coords, aes(x="x", y="y", fill = "edits"))
    + geom_tile()
    + scale_fill_cmap(name = "Edits")
    + coord_fixed()
    + labs(title="Pixel Activity Heat map for 04-03 - 21:00:00",
           x="X Coordinate",
           y="Y Coordinate")
    + theme_minimal()
)
g_coord.show()

user_placement = (con.execute("""
    SELECT DATE_TRUNC('minute', ts) AS minute,
           COUNT(DISTINCT user_id) AS active_users
    FROM parquet_scan(place_preprocessed.parquet)
    GROUP BY minute
    ORDER BY active_users DESC
""").df())

print(user_placement.head())

first_users = con.execute("""
    SELECT COUNT(*) AS first_time_users
    FROM (
      SELECT user_id, DATE_TRUNC('minute', MIN(ts)) AS first_minute
      FROM parquet_scan('place_preprocessed.parquet')
      GROUP BY user_id
    ) u
    WHERE u.first_minute = TIMESTAMP '2022-04-03 21:00:00';
""").df()

avg_first_users = con.execute("""
    SELECT AVG(c.first_time_users) as mean_first_users_per_minute, STDDEV_SAMP(c.first_time_users) as std_first_users_per_min
    FROM(   
        SELECT COUNT(u.user_id) AS first_time_users
        FROM (
          SELECT user_id, DATE_TRUNC('minute', MIN(ts)) AS first_minute
          FROM parquet_scan('place_preprocessed.parquet')
          GROUP BY user_id
        ) u
        GROUP BY u.first_minute
        ) c
    ;
""").df()

print(first_users)
print(avg_first_users)

spike_count = first_users["first_time_users"][0]
mean_count = avg_first_users["mean_first_users_per_minute"][0]
std_count = avg_first_users["std_first_users_per_min"][0]

z_score = (spike_count - mean_count) / std_count

spike_summary = pd.DataFrame({
    "Spike Minute": ["2022-04-03 21:00"],
    "First-Time Users in Spike": [spike_count],
    "Average First-Time Users per Minute": [mean_count],
    "Std Dev per Minute": [std_count],
    "Z-Score of Spike": [z_score]
})

print(spike_summary.to_markdown(index=False))

spike_minute = "2022-04-03 21:00:00"

lifespans = con.execute(f"""
    WITH user_bounds AS (
      SELECT
        user_id,
        MIN(ts) AS first_ts,
        MAX(ts) AS last_ts
      FROM parquet_scan('place_preprocessed.parquet')
      GROUP BY user_id
    ),
    spike_new_users AS (
      SELECT *
      FROM user_bounds
      WHERE DATE_TRUNC('minute', first_ts) = TIMESTAMP '{spike_minute}'
    )
    SELECT
      user_id,
      first_ts,
      last_ts,
      DATE_DIFF('minute', first_ts, last_ts) AS lifespan_minutes,
      DATE_DIFF('second', first_ts, last_ts) AS lifespan_seconds
    FROM spike_new_users
    ORDER BY lifespan_minutes DESC;
""").df()


summary = con.execute(f"""
    WITH user_bounds AS (
      SELECT user_id, MIN(ts) AS first_ts, MAX(ts) AS last_ts
      FROM parquet_scan('place_preprocessed.parquet')
      GROUP BY user_id
    ),
    spike_new_users AS (
      SELECT *
      FROM user_bounds
      WHERE DATE_TRUNC('minute', first_ts) = TIMESTAMP '{spike_minute}'
    )
    SELECT
      COUNT(*) AS spike_new_users,
      AVG(DATE_DIFF('minute', first_ts, last_ts)) AS avg_lifespan_min,
      STDDEV_SAMP(DATE_DIFF('minute', first_ts, last_ts)) AS std_lifespan_min,
      PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY DATE_DIFF('minute', first_ts, last_ts)) AS median_lifespan_min,
      PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY DATE_DIFF('minute', first_ts, last_ts)) AS p90_lifespan_min
    FROM spike_new_users;
""").df()

print(summary)

(
    ggplot(lifespans, aes(x="lifespan_minutes"))
    + geom_histogram(bins=50)
    + labs(title="Lifespan of Users Who First Appeared During Spike",
           x="Lifespan (minutes)",
           y="Number of Users")
    + theme_minimal()
).show()

short_users = lifespans[lifespans["lifespan_minutes"] <= 5]["user_id"]
long_users  = lifespans[lifespans["lifespan_minutes"] >= 1000]["user_id"]

short_df = con.execute(f"""
    SELECT x, y
    FROM parquet_scan('place_preprocessed.parquet')
    WHERE user_id IN ({','.join(map(str, short_users.tolist()))})
""").df()

long_df = con.execute(f"""
    SELECT x, y
    FROM parquet_scan('place_preprocessed.parquet')
    WHERE user_id IN ({','.join(map(str, long_users.tolist()))})
""").df()

(
    ggplot(short_df, aes(x="x", y="y"))
    + geom_bin2d(bins=100)
    + coord_fixed()
    + labs(title="Pixel Activity — Short-Lived Users",
           x="X", y="Y")
    + theme_minimal()
).show()

(
    ggplot(long_df, aes(x="x", y="y"))
    + geom_bin2d(bins=100)
    + coord_fixed()
    + labs(title="Pixel Activity — Long-Lived Users",
           x="X", y="Y")
    + theme_minimal()
).show()


