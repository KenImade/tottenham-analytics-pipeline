import os
from dagster import asset, AssetExecutionContext, MetadataValue
import pandas as pd


@asset(
    description="Find the player who took shots from the furthest distance from goal",
    group_name="sql_analysis",
    compute_kind="sql",
    deps=["load_match_data"],  # Depends on data being loaded first
    required_resource_keys={"football_config", "postgres"},
)
def furthest_shot_from_goal(context: AssetExecutionContext) -> pd.DataFrame:
    """
    Finds the player who took the furthest shot from goal centre (120, 40).
    """

    query = """
        SELECT
            event_id,
            match_id,
            team_name,
            player_name,
            timestamp as time_of_shot,
            location_x,
            location_y,
            ROUND(CAST(SQRT(POWER(location_x - 120, 2) + POWER(location_y - 40, 2)) AS numeric), 2) AS shot_distance_from_goal
        FROM events
        WHERE event_type = 'Shot'
        AND location_x IS NOT NULL
        AND location_y IS NOT NULL
        ORDER BY shot_distance_from_goal DESC
        LIMIT 1
    """

    conn = context.resources.postgres.get_connection()

    try:
        df = pd.read_sql_query(query, conn)

        if len(df) > 0:
            context.add_output_metadata(
                {
                    "player": df.iloc[0]["player_name"],
                    "team": df.iloc[0]["team_name"],
                    "distance": float(round(df.iloc[0]["shot_distance_from_goal"], 2)),
                    "location": f"({df.iloc[0]['location_x']}, {df.iloc[0]['location_y']})",
                    "time_of_shot": (
                        df.iloc[0]["time_of_shot"]
                        if df.iloc[0]["time_of_shot"]
                        else None
                    ),
                    "preview": MetadataValue.md(df.to_markdown(index=False)),
                }
            )
        else:
            context.add_output_metadata({"message": "No shots found in the data"})

        output_dir = context.resources.football_config.output_dir
        os.makedirs(output_dir, exist_ok=True)
        df.to_csv(
            os.path.join(output_dir, "3f_furthest_shot_from_goal.csv"), index=False
        )

        return df

    finally:
        conn.close()


@asset(
    description="Find all events that occurred in the penalty box, grouped by type",
    group_name="sql_analysis",
    compute_kind="sql",
    deps=["load_match_data"],
    required_resource_keys={"football_config", "postgres"},
)
def penalty_box_events(
    context: AssetExecutionContext,
) -> pd.DataFrame:
    """
    Finds all events in the penalty box (x > 102, y between 18 and 62).
    """

    query = """
        SELECT
            event_type,
            COUNT(*) AS event_count
        FROM events
        WHERE location_x IS NOT NULL
        AND location_y IS NOT NULL
        AND location_x > 102
        AND location_y BETWEEN 18 AND 62
        GROUP BY event_type
        ORDER BY event_count DESC;
    """

    conn = context.resources.postgres.get_connection()

    try:
        df = pd.read_sql_query(query, conn)

        if len(df) > 0:
            total_events = int(df["event_count"].sum())
            most_common_event = df.iloc[0]["event_type"]

            context.add_output_metadata(
                {
                    "total_penalty_box_events": total_events,
                    "unique_event_types": len(df),
                    "most_common_event": most_common_event,
                    "most_common_event_count": int(df.iloc[0]["event_count"]),
                    "preview": MetadataValue.md(df.to_markdown(index=False)),
                }
            )
        else:
            context.add_output_metadata({"message": "No events found in penalty box"})

        output_dir = context.resources.football_config.output_dir
        os.makedirs(output_dir, exist_ok=True)
        df.to_csv(os.path.join(output_dir, "3g_penalty_box_events.csv"), index=False)

        return df

    finally:
        conn.close()


@asset(
    description="Find the longest time gap between consecutive shots in the match",
    group_name="sql_analysis",
    compute_kind="sql",
    deps=["load_match_data"],
    required_resource_keys={"football_config", "postgres"},
)
def longest_shot_gap(context: AssetExecutionContext) -> pd.DataFrame:
    """
    Finds the longest time gap between any two consecutive shots in the match,
    regardless of team. Returns both shot timestamps, players, teams, and the time difference.
    """

    query = """
        WITH shot_events AS (
            SELECT
                match_id,
                player_name,
                team_name,
                CAST(timestamp AS time) AS shot_time
            FROM events
            WHERE event_type = 'Shot'
              AND timestamp IS NOT NULL
        ),
        shot_gaps AS (
            SELECT
                match_id,
                player_name AS second_shot_player,
                team_name  AS second_shot_team,
                shot_time,
                LAG(player_name) OVER (PARTITION BY match_id ORDER BY shot_time) AS first_shot_player,
                LAG(team_name)  OVER (PARTITION BY match_id ORDER BY shot_time) AS first_shot_team,
                LAG(shot_time)  OVER (PARTITION BY match_id ORDER BY shot_time) AS prev_shot_time,
                EXTRACT(EPOCH FROM (shot_time - LAG(shot_time) OVER (PARTITION BY match_id ORDER BY shot_time))) AS time_gap_seconds
            FROM shot_events
        )
        SELECT
            match_id,
            first_shot_player,
            first_shot_team,
            second_shot_player,
            second_shot_team,
            prev_shot_time AS previous_shot,
            shot_time      AS next_shot,
            time_gap_seconds,
            ROUND(time_gap_seconds / 60.0, 2) AS time_gap_minutes
        FROM shot_gaps
        WHERE time_gap_seconds IS NOT NULL
        ORDER BY time_gap_seconds DESC
        LIMIT 1;
    """

    conn = context.resources.postgres.get_connection()

    try:
        df = pd.read_sql_query(query, conn)

        if not df.empty:
            context.add_output_metadata(
                {
                    "longest_gap_seconds": float(df.iloc[0]["time_gap_seconds"]),
                    "longest_gap_minutes": float(df.iloc[0]["time_gap_minutes"]),
                    "first_shot_player": df.iloc[0]["first_shot_player"],
                    "first_shot_team": df.iloc[0]["first_shot_team"],
                    "second_shot_player": df.iloc[0]["second_shot_player"],
                    "second_shot_team": df.iloc[0]["second_shot_team"],
                    "preview": MetadataValue.md(df.to_markdown(index=False)),
                }
            )
        else:
            context.add_output_metadata(
                {"message": "Not enough shots to calculate gap"}
            )
        output_dir = context.resources.football_config.output_dir
        os.makedirs(output_dir, exist_ok=True)
        df.to_csv(os.path.join(output_dir, "3h_longest_shot_gap.csv"), index=False)

        return df

    finally:
        conn.close()
