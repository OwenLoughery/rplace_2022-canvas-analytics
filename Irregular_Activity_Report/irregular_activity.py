import duckdb
import pandas as pd
from plotnine import *
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)



con = duckdb.connect()



#######################
#######################
# BUCKET ONE
#######################
#######################

####################################################################################
# PIXEL CHURN SECTION EXPLORING IF POSSIBLE IRREGULAR ACTIVITY AREA
####################################################################################



pixel_churn = con.execute("""
    SELECT x, y, COUNT(*) AS edits
    FROM parquet_scan('place_preprocessed.parquet')
    GROUP BY x, y
""").df()

print(pixel_churn.head(5).to_markdown(index=False))


(
    ggplot(pixel_churn, aes(x="x", y="y", fill="edits"))
    + geom_tile()
    + scale_fill_cmap(name="Edits (Churn)")
    + coord_fixed()
    + labs(title="Heavily Contested Pixels Heatmap — Total Edits per Pixel",
           x="X Coordinate", y="Y Coordinate")
    + theme_minimal()
).show()

high_churn = con.execute("""
    SELECT x, y, COUNT(*) AS edits
    FROM parquet_scan('place_preprocessed.parquet')
    GROUP BY x, y
    HAVING COUNT(*) > 50
    ORDER BY edits DESC
""").df()

print(high_churn.head(5).to_markdown(index=False))

top_pixels = con.execute("""
    SELECT x, y
    FROM parquet_scan('place_preprocessed.parquet')
    GROUP BY x, y
    HAVING COUNT(*) > 100
""").df()

con.register("top_pixels", top_pixels)

churn_users = con.execute("""
    SELECT p.user_id, COUNT(*) AS edits_on_hot_pixels
    FROM parquet_scan('place_preprocessed.parquet') p
    JOIN top_pixels t
      ON p.x = t.x AND p.y = t.y
    GROUP BY p.user_id
    ORDER BY edits_on_hot_pixels DESC
""").df()

print(churn_users.head(5).to_markdown(index=False))




####################################################################################
# TIME BETWEEN PLACEMENT SECTION
####################################################################################



con.execute("""
CREATE OR REPLACE TEMP TABLE top_churn_users AS
SELECT user_id
FROM churn_users
LIMIT 50
""")

timing = con.execute("""
    SELECT
        user_id,
        ts,
        ts - LAG(ts) OVER (PARTITION BY user_id ORDER BY ts) AS delta
    FROM parquet_scan('place_preprocessed.parquet')
    WHERE user_id IN (SELECT user_id FROM top_churn_users)
""").df()

timing["delta_seconds"] = timing["delta"].dt.total_seconds()

(
    ggplot(timing.dropna(), aes(x="delta_seconds"))
    + geom_histogram(bins=100)
    + scale_x_log10()
    + labs(title="Time Between Placement for \n Heavily Contested Pixels Area Users",
           x="Seconds Between Placements",
           y="Count")
    + theme_minimal()
).show()


####################################################################################
# SESSION OF ACTIVITY TIME COMPARISON SECTION
####################################################################################


sessions = con.execute("""
    WITH deltas AS (
      SELECT
        user_id,
        ts,
        ts - LAG(ts) OVER (PARTITION BY user_id ORDER BY ts) AS delta
      FROM parquet_scan('place_preprocessed.parquet')
      WHERE user_id IN (SELECT user_id FROM top_churn_users)
    ),
    session_flags AS (
      SELECT *,
             CASE WHEN delta IS NULL OR delta > INTERVAL 30 MINUTE THEN 1 ELSE 0 END AS new_session
      FROM deltas
    ),
    session_ids AS (
      SELECT *,
             SUM(new_session) OVER (PARTITION BY user_id ORDER BY ts) AS session_id
      FROM session_flags
    )
    SELECT
      user_id,
      session_id,
      MIN(ts) AS start_ts,
      MAX(ts) AS end_ts,
      DATE_DIFF('minute', MIN(ts), MAX(ts)) AS session_length_min
    FROM session_ids
    GROUP BY user_id, session_id
""").df()

(
    ggplot(sessions, aes(x="session_length_min"))
    + geom_histogram(bins=50)
    + scale_x_log10()
    + labs(title="Continuous Activity Session Lengths for \n Top Contested Pixels Area Users",
           x="Session Length (minutes)",
           y="Count")
    + theme_minimal()
).show()


sessions_overall = con.execute("""
    WITH deltas AS (
      SELECT
        user_id,
        ts,
        ts - LAG(ts) OVER (PARTITION BY user_id ORDER BY ts) AS delta
      FROM parquet_scan('place_preprocessed.parquet')
    ),
    session_flags AS (
      SELECT *,
             CASE WHEN delta IS NULL OR delta > INTERVAL 30 MINUTE THEN 1 ELSE 0 END AS new_session
      FROM deltas
    ),
    session_ids AS (
      SELECT *,
             SUM(new_session) OVER (PARTITION BY user_id ORDER BY ts) AS session_id
      FROM session_flags
    )
    SELECT
      user_id,
      session_id,
      MIN(ts) AS start_ts,
      MAX(ts) AS end_ts,
      DATE_DIFF('minute', MIN(ts), MAX(ts)) AS session_length_min
    FROM session_ids
    GROUP BY user_id, session_id
""").df()

(
    ggplot(sessions_overall, aes(x="session_length_min"))
    + geom_histogram(bins=50)
    + scale_x_log10()
    + labs(title="Continuous Activity Session Lengths from r/place",
           x="Session Length (minutes)",
           y="Count")
    + theme_minimal()
).show()


####################################################################################
# FRACTION OF EDITS IN HIGH CHURN PIXELS
####################################################################################



con.execute("""
CREATE OR REPLACE TEMP TABLE hot_pixels AS
SELECT x, y
FROM parquet_scan('place_preprocessed.parquet')
GROUP BY x, y
HAVING COUNT(*) > 1000
""")

focus = con.execute("""
    WITH user_totals AS (
      SELECT user_id, COUNT(*) AS total_edits
      FROM parquet_scan('place_preprocessed.parquet')
      WHERE user_id IN (SELECT user_id FROM top_churn_users)
      GROUP BY user_id
    ),
    user_hot AS (
      SELECT p.user_id, COUNT(*) AS hot_edits
      FROM parquet_scan('place_preprocessed.parquet') p
      JOIN hot_pixels h ON p.x=h.x AND p.y=h.y
      WHERE p.user_id IN (SELECT user_id FROM top_churn_users)
      GROUP BY p.user_id
    )
    SELECT
      t.user_id,
      total_edits,
      COALESCE(hot_edits,0) AS hot_edits,
      COALESCE(hot_edits,0) * 1.0 / total_edits AS hot_edit_ratio
    FROM user_totals t
    LEFT JOIN user_hot h USING(user_id)
    ORDER BY hot_edit_ratio DESC
""").df()

(
    ggplot(focus, aes(x="hot_edit_ratio"))
    + geom_histogram(bins=30)
    + labs(title="Fraction of Edits in Top Contested Pixels",
           x="Proportion of User's Edits in Hot Pixels",
           y="Number of Users")
    + theme_minimal()
).show()




#######################
#######################
# BUCKET TWO
#######################
#######################

####################################################################################
# LOW AMOUNT OF COLOR CHOICE SECTION EXPLORING IF POSSIBLE IRREGULAR ACTIVITY AREA
####################################################################################



color_counts = con.execute("""
    WITH color_count AS (
        SELECT
            user_id,
            COUNT(DISTINCT pc) AS total_colors,
            COUNT(*) AS total_edits
        FROM parquet_scan('place_preprocessed.parquet')
        GROUP BY user_id
    ),
    low_entropy AS (
        SELECT 
            c.user_id,
            c.total_colors,
            c.total_edits,
            (c.total_colors * 1.0) / NULLIF(c.total_edits, 0) AS color_diversity_ratio
        FROM color_count c
        WHERE c.total_edits > 200
          AND c.total_colors <= 2
    )
    SELECT 
        user_id,
        total_colors,
        total_edits,
        color_diversity_ratio
    FROM low_entropy
    ORDER BY color_diversity_ratio ASC, total_edits DESC
""").df()


print(color_counts.head(10))



con.register("low_entropy_users", color_counts)

low_entropy_pixels = con.execute("""
SELECT x, y
FROM parquet_scan('place_preprocessed.parquet')
WHERE user_id IN (SELECT user_id FROM low_entropy_users)
""").df()

(
    ggplot(low_entropy_pixels, aes(x="x", y="y"))
    + geom_bin2d(bins=120)
    + coord_fixed()
    + labs(title="Pixel Locations from Low Color Diversity Users",
           x="X Coordinate",
           y="Y Coordinate")
    + theme_minimal()
).show()

top_5_users = color_counts.head(5)
top5_ids = top_5_users["user_id"].tolist()

con.register("top_5_users", top_5_users)

user_pixels = con.execute(
    """
    SELECT user_id, x, y
    FROM parquet_scan('place_preprocessed.parquet')
    WHERE user_id IN (SELECT user_id FROM top_5_users)
    """
).df()

(
    ggplot(user_pixels, aes(x="x", y="y"))
    + geom_bin2d(bins=80)
    + coord_fixed()
    + facet_wrap("~user_id")
    + labs(title="Pixel Placement Patterns of Top 5 Low Color Diversity Users")
    + theme_minimal()
).show()

print(color_counts.head(5).to_markdown(index=False))



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



