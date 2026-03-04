import duckdb
import time
from datetime import datetime
import sys

def analysis(start, end):

    def ms(ns):
        return ns / 1_000_000

    hex_dict = {
        "#000000": "Black",
        "#00756F": "Dark Teal",
        "#009EAA": "Teal",
        "#00A368": "Dark Green",
        "#00CC78": "Green",
        "#00CCC0": "Cyan",
        "#2450A4": "Dark Blue",
        "#3690EA": "Blue",
        "#493AC1": "Indigo",
        "#515252": "Dark Gray",
        "#51E9F4": "Light Cyan",
        "#6A5CFF": "Periwinkle",
        "#6D001A": "Dark Red",
        "#6D482F": "Brown",
        "#7EED56": "Light Green",
        "#811E9F": "Purple",
        "#898D90": "Gray",
        "#94B3FF": "Light Periwinkle",
        "#9C6926": "Dark Brown",
        "#B44AC0": "Lavender",
        "#BE0039": "Red",
        "#D4D7D9": "Light Gray",
        "#DE107F": "Magenta",
        "#E4ABFF": "Light Lavender",
        "#FF3881": "Pink",
        "#FF4500": "Orange",
        "#FF99AA": "Light Pink",
        "#FFA800": "Gold",
        "#FFB470": "Peach",
        "#FFD635": "Yellow",
        "#FFF8B8": "Light Yellow",
        "#FFFFFF": "White",
    }

    case_sql = "CASE pc\n"
    for hex_code, name in hex_dict.items():
        case_sql += f"    WHEN '{hex_code}' THEN '{name}'\n"
    case_sql += "END AS color"

    start_t = datetime.strptime(start, "%Y-%m-%d %H")
    end_t = datetime.strptime(end, "%Y-%m-%d %H")
    if end_t <= start_t:
        raise ValueError("End hour must be after start hour.")

    con = duckdb.connect()

    start_sql = start_t.strftime("%Y-%m-%d %H:%M:%S")
    end_sql = end_t.strftime("%Y-%m-%d %H:%M:%S")

    con.execute(f"""
        CREATE OR REPLACE VIEW place AS
        SELECT
            ts,
            user_id,
            x,
            y,
            {case_sql}
        FROM parquet_scan('place_preprocessed.parquet')
        WHERE ts >= TIMESTAMP '{start_sql}'
          AND ts <  TIMESTAMP '{end_sql}';
    """)

    t0 = time.perf_counter_ns()
    color_rank = con.execute(
        """
        SELECT
            color,
            COUNT(DISTINCT user_id) AS user_count
        FROM place
        GROUP BY color
        ORDER BY user_count DESC;
        """
    ).fetchall()
    t1 = time.perf_counter_ns()
    color_rank_ms = ms(t1 - t0)



    t0 = time.perf_counter_ns()
    avg_session_seconds = con.execute("""
    WITH base AS (
        SELECT
            user_id,
            ts
        FROM place
    ),
    eligible AS (
        SELECT user_id
        FROM base
        GROUP BY user_id
        HAVING COUNT(*) > 1
    ),
    ordered AS (
        SELECT
            b.user_id,
            b.ts,
            LAG(b.ts) OVER (PARTITION BY b.user_id ORDER BY b.ts) AS prev_ts
        FROM base b
        JOIN eligible e USING (user_id)
    ),
    marked AS (
        SELECT
            user_id,
            ts,
            CASE
                WHEN prev_ts IS NULL THEN 1
                WHEN ts > prev_ts + INTERVAL '15 minutes' THEN 1
                ELSE 0
            END AS new_session
        FROM ordered
    ),
    sessionized AS (
        SELECT
            user_id,
            ts,
            SUM(new_session) OVER (
                PARTITION BY user_id
                ORDER BY ts
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS session_id
        FROM marked
    ),
    sessions AS (
        SELECT
            user_id,
            session_id,
            date_diff('second', MIN(ts), MAX(ts)) AS session_len_seconds
        FROM sessionized
        GROUP BY user_id, session_id
    )
    SELECT AVG(session_len_seconds) AS avg_session_seconds
    FROM sessions;
    """).fetchone()[0]
    t1 = time.perf_counter_ns()
    avg_session_ms = ms(t1 - t0)



    t0 = time.perf_counter_ns()
    percentiles = con.execute(
        """
        SELECT
            quantile(color_count, 0.5) AS "50th Percentile",
            quantile(color_count, 0.75) AS "75th Percentile",
            quantile(color_count, 0.9) AS "90th Percentile",
            quantile(color_count, 0.99) AS "99th Percentile"
        FROM (
            SELECT
                user_id,
                COUNT(*) AS color_count
            FROM place
            GROUP BY user_id
        ) t;
        """
    ).fetchall()
    t1 = time.perf_counter_ns()
    percentiles_ms = ms(t1 - t0)



    t0 = time.perf_counter_ns()
    first_users = con.execute(
        """
        SELECT COUNT(*) AS first_time_users
        FROM (
          SELECT user_id, MIN(ts) AS first_ts
          FROM parquet_scan('place_preprocessed.parquet')
          GROUP BY user_id
        ) u
        WHERE u.first_ts >= ? AND u.first_ts < ?;
        """,
        [start_t, end_t],
    ).fetchone()[0]
    t1 = time.perf_counter_ns()
    first_users_ms = ms(t1 - t0)

    total_runtime = first_users_ms + percentiles_ms + avg_session_ms + color_rank_ms

    print(f"Timeframe: {start} to {end}")

    print("\nRanking of Colors by Distinct Users:")
    for i, (color, user_count) in enumerate(color_rank, start=1):
        print(f"  {i}. {color}: {user_count} users")

    print("\nAverage Session Length:")
    print(f"  Output: {avg_session_seconds:.2f} seconds")

    p50, p75, p90, p99 = percentiles[0]
    print("\nPercentiles of Pixels Placed (per user):")
    print(f"  50th Percentile: {p50}")
    print(f"  75th Percentile: {p75}")
    print(f"  90th Percentile: {p90}")
    print(f"  99th Percentile: {p99}")

    print("\nCount of First-Time Users:")
    print(f"  Output: {first_users} users")

    print("\nRuntime:")
    print(f"{total_runtime:.2f} ms")

def main():
    if len(sys.argv) != 3:
        raise SystemExit(
            'Usage: python analysis_week3.py "YYYY-MM-DD HH" "YYYY-MM-DD HH"'
        )

    analysis(sys.argv[1], sys.argv[2])

if __name__ == "__main__":
    main()
