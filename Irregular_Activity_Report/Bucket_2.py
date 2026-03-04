import duckdb
import pandas as pd
from plotnine import *
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)



con = duckdb.connect()


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

