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


