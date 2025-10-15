import os
from dagster import asset, AssetExecutionContext, MetadataValue
from typing import List, Dict, Any
import pandas as pd
from src.matchday_pipeline.defs.utils import parse_timestamp_to_seconds


@asset(
    description="Calculate time on pitch for each player in seconds",
    group_name="analysis",
    compute_kind="python",
    required_resource_keys={"football_config"},
)
def player_time_on_pitch(
    context: AssetExecutionContext, validated_lineup: List[Dict[str, Any]]
) -> pd.DataFrame:
    """
    Calculate seconds each player spent on the field based on lineup positions

    Args:
        context: Dagster execution context
        validated_lineup: List of dictionaries containing team lineup data with player positions

    Returns:
        DataFrame with player_id, player_name, team, seconds_on_pitch, and played_match columns
    """

    player_times = []

    # Process each team's lineup
    for team_data in validated_lineup:
        team_name = team_data.get("team_name", "Unknown")

        # Get match end time from events for this match
        match_end_time = None
        match_id = team_data.get("match_id")

        for event in team_data.get("events", []):
            if event["type"] == "Half End":
                event_time = parse_timestamp_to_seconds(event["timestamp"])

                if match_end_time is None or event_time > match_end_time:
                    match_end_time = event_time

        # Process each player in the team
        for player in team_data.get("lineup", []):
            player_id = player["player_id"]
            player_name = player.get("player_name", "Unknown")
            total_time = 0

            # Calculate time for each position period
            for position in player.get("positions", []):
                if position:
                    start_time = parse_timestamp_to_seconds(position["from"])
                    end_time = position["to"]

                    # If end_time is None, use match end time
                    if end_time is None:
                        if match_end_time is not None:
                            end_time_seconds = match_end_time
                        else:
                            # Default to approximate full match time if not available
                            end_time_seconds = 90 * 60
                    else:
                        end_time_seconds = parse_timestamp_to_seconds(end_time)

                    # Calculate duration for this position period
                    duration = end_time_seconds - start_time
                    total_time += duration

            # Store result
            player_times.append(
                {
                    "player_id": player_id,
                    "player_name": player_name,
                    "team": team_name,
                    "match_id": match_id,
                    "seconds_on_pitch": float(round(total_time, 2)),
                    "minutes_on_pitch": float(round(total_time / 60, 2)),
                    "played_match": total_time > 0,
                }
            )

    # Create DataFrame
    df = pd.DataFrame(player_times)

    # Calculate metadata
    total_players = len(df)
    players_who_played = int(df["played_match"].sum())
    avg_time_on_pitch = (
        float(round(df["seconds_on_pitch"].mean(), 2)) if total_players > 0 else 0.0
    )

    context.add_output_metadata(
        {
            "total_players": total_players,
            "players_who_played": players_who_played,
            "players_on_bench": total_players - players_who_played,
            "avg_time_on_pitch_seconds": avg_time_on_pitch,
            "avg_time_on_pitch_minutes": float(round(avg_time_on_pitch / 60, 2)),
            "max_time_on_pitch": (
                float(df["seconds_on_pitch"].max()) if total_players > 0 else 0.0
            ),
            "min_time_on_pitch": (
                float(df[df["played_match"]]["seconds_on_pitch"].min())
                if players_who_played > 0
                else 0.0
            ),
            "preview": MetadataValue.md(df.head(50).to_markdown(index=False)),
        }
    )

    output_dir = context.resources.football_config.output_dir
    os.makedirs(output_dir, exist_ok=True)
    df.to_csv(os.path.join(output_dir, "2a_player_time_on_pitch.csv"), index=False)

    return df


@asset(
    description="Calculate total match duration in milliseconds",
    group_name="analysis",
    compute_kind="python",
    required_resource_keys={"football_config"},
)
def calculate_match_duration(
    context: AssetExecutionContext, validated_events: List[Dict[str, Any]]
) -> pd.DataFrame:
    """
    Calculates the total match duration in milliseconds

    Args:
        context: Dagster execution context
        validated_events: List of dictionaires containing match events data

    Returns:
        Dataframe with total match duration in milliseconds, seconds, and minutes
    """
    # Extract match metadata from first event
    match_id = validated_events[0].get("match_id") if validated_events else None

    period_durations = {}

    # Get all periods that occurred during the match and have a type of Half End i.e. 1st Half, 2nd Half, 3rd Period, etc.
    for event_data in validated_events:
        event_type = event_data.get("type", "Unknown").get("name", "Unknown")

        if event_type == "Half End":
            period = event_data.get("period")
            timestamp = event_data.get("timestamp", "00:00:00.000")

            if period is not None:
                timestamp_seconds = parse_timestamp_to_seconds(timestamp)
                period_durations[period] = timestamp_seconds

    # Sum the total time for each period together
    total_time = 0
    sorted_periods = sorted(period_durations.keys())

    for _, period in enumerate(sorted_periods):
        total_time += period_durations[period]

    total_time_milliseconds = total_time * 1000

    # Create DataFrame
    df = pd.DataFrame(
        [
            {
                "match_id": match_id,
                "num_periods": len(period_durations),
                "total_match_duration_seconds": float(round(total_time, 3)),
                "total_match_duration_milliseconds": float(
                    round(total_time_milliseconds, 3)
                ),
                "total_match_duration_minutes": float(round(total_time / 60, 2)),
            }
        ]
    )

    # Create metadata
    context.add_output_metadata(
        {
            "match_id": match_id,
            "total_events_processed": len(validated_events),
            "num_periods": len(period_durations),
            "match_duration_seconds": float(round(total_time, 3)),
            "match_duration_minutes": float(round(total_time / 60, 2)),
            "match_duration_milliseconds": float(round(total_time_milliseconds, 3)),
            "period_details": MetadataValue.json(
                {
                    f"period_{p}": float(round(period_durations[p], 2))
                    for p in sorted_periods
                }
            ),
            "previews": MetadataValue.md(df.to_markdown(index=False)),
        }
    )

    output_dir = context.resources.football_config.output_dir
    os.makedirs(output_dir, exist_ok=True)
    df.to_csv(os.path.join(output_dir, "2b_match_duration.csv"), index=False)

    # Return dataframe
    return df


@asset(
    description="Calculate total number of pass events for each player",
    group_name="analysis",
    compute_kind="python",
    required_resource_keys={"football_config"},
)
def total_passes_per_player(
    context: AssetExecutionContext,
    validated_events: List[Dict[str, Any]],
    validated_lineup: List[Dict[str, Any]],
) -> pd.DataFrame:
    """
    Calculates the total number of pass events for each player.

    Args:
        context: Dagster execution context
        validated_events: List of dictionaires containing match events data
        validated_lineup: List of dictionaires containing match lineup data

    Returns:
        Dataframe containing passing data for each player.
    """
    # Initialize player passes for each player
    player_passes = {}

    # Count pass events that occurred during the match for each player
    for event in validated_events:
        event_type = event.get("type", {}).get("name", "")

        # Check event type is a pass
        if event_type == "Pass":
            player_id = event.get("player", {}).get("id")
            player_name = event.get("player", {}).get("name", "Unknown")
            team_name = event.get("team", {}).get("name", "Unknown")

            if player_id is not None:
                # Initialize player entry if not exists
                if player_id not in player_passes:
                    player_passes[player_id] = {
                        "player_name": player_name,
                        "team_name": team_name,
                        "total_passes": 0,
                    }

                # Increment total passes
                player_passes[player_id]["total_passes"] += 1

    # Get all players from lineup data to reflect those with 0 passes. i.e did not play the match
    for team_data in validated_lineup:
        team_name = team_data.get("team_name", "Unknown")

        for player in team_data.get("lineup", []):
            player_id = player.get("player_id")
            player_name = player.get("player_name", "Unknown")

            # Add player if they haven't made any passes
            if player_id not in player_passes:
                player_passes[player_id] = {
                    "player_name": player_name,
                    "team_name": team_name,
                    "total_passes": 0,
                }

    # Append to passing data list
    passing_data = []

    for player_id, data in player_passes.items():
        passing_data.append(
            {
                "player_id": player_id,
                "player_name": data["player_name"],
                "team_name": data["team_name"],
                "total_passes": data["total_passes"],
            }
        )

    # Create Dataframe
    df = pd.DataFrame(passing_data)

    # Sort by total passes descending
    df = df.sort_values("total_passes", ascending=False).reset_index(drop=True)

    # Create metadata
    if len(df) > 0:
        total_pass_events = int(df["total_passes"].sum())
        players_with_passes = int((df["total_passes"] > 0).sum())
        avg_passes_per_player = float(round(df["total_passes"].mean(), 2))

        context.add_output_metadata(
            {
                "total_players": len(df),
                "players_with_passes": players_with_passes,
                "players_without_passes": len(df) - players_with_passes,
                "total_pass_events": total_pass_events,
                "avg_passes_per_player": avg_passes_per_player,
                "max_passes_by_player": int(df["total_passes"].max()),
                "top_passer": df.iloc[0]["player_name"] if len(df) > 0 else "N/A",
                "preview": MetadataValue.md(df.head(20).to_markdown(index=False)),
            }
        )
    else:
        context.add_output_metadata(
            {"total_players": 0, "message": "No passing data available"}
        )

    output_dir = context.resources.football_config.output_dir
    os.makedirs(output_dir, exist_ok=True)
    df.to_csv(os.path.join(output_dir, "2c_total_passes_per_player.csv"), index=False)

    return df


@asset(
    description="Calculate at what minute each goal was scored",
    group_name="analysis",
    compute_kind="python",
    required_resource_keys={"football_config"},
)
def get_goal_minutes(
    context: AssetExecutionContext, validated_events: List[Dict[str, Any]]
) -> pd.DataFrame:
    """
    Extracts the minutes goals were scored in the match.

    Args:
        validated_events: List of event dictionaries

    Returns:
        DataFrame containing goal timing data.
    """

    goals = []
    processed_own_goals = set()  # Track own goals to avoid duplicates

    # Look for goal-related events
    for event in validated_events:
        event_type = event.get("type", {}).get("name", "")

        # Check for different types of goals
        # Shot events with goal outcome, or Own Goal events
        is_goal = False
        goal_type = None

        if event_type == "Shot":
            outcome = event.get("shot", {}).get("outcome", {}).get("name")
            if outcome == "Goal":
                is_goal = True
                goal_type = "Goal"
        elif event_type == "Own Goal Against":
            # Only process "Own Goal Against" to avoid counting twice
            # Create unique identifier for this own goal
            timestamp = event.get("timestamp", "00:00:00.000")
            period = event.get("period", 1)
            own_goal_key = f"{period}_{timestamp}"

            if own_goal_key not in processed_own_goals:
                is_goal = True
                goal_type = "Own Goal"
                processed_own_goals.add(own_goal_key)
        elif event_type == "Own Goal For":
            # Skip "Own Goal For" as it's the duplicate
            continue

        if is_goal:
            # Extract goal information
            timestamp = event.get("timestamp", "00:00:00.000")
            timestamp_seconds = parse_timestamp_to_seconds(timestamp)

            # Calculate minute (rounded down)
            minute = int(timestamp_seconds // 60)
            second = int(timestamp_seconds % 60)

            # Get match minute from event (if available)
            match_minute = event.get("minute")
            match_second = event.get("second")

            player_id = event.get("player", {}).get("id")
            player_name = event.get("player", {}).get("name", "Unknown")
            team_name = event.get("team", {}).get("name", "Unknown")
            period = event.get("period", 1)

            # Get additional context
            possession_team = event.get("possession_team", {}).get("name", "Unknown")
            play_pattern = event.get("play_pattern", {}).get("name", "Unknown")

            # For shots, get additional shot details
            shot_technique = None
            shot_body_part = None
            if event_type == "Shot":
                shot_technique = event.get("shot", {}).get("technique", {}).get("name")
                shot_body_part = event.get("shot", {}).get("body_part", {}).get("name")

            goals.append(
                {
                    "player_id": player_id,
                    "player_name": player_name,
                    "team_name": team_name,
                    "goal_type": goal_type,
                    "period": period,
                    "minute": match_minute if match_minute is not None else minute,
                    "second": match_second if match_second is not None else second,
                    "timestamp": timestamp,
                    "timestamp_seconds": float(round(timestamp_seconds, 3)),
                    "match_time": f"{minute}:{second:02d}",
                    "possession_team": possession_team,
                    "play_pattern": play_pattern,
                    "shot_technique": shot_technique,
                    "shot_body_part": shot_body_part,
                }
            )

    # Create DataFrame
    df = pd.DataFrame(goals)

    # Sort by timestamp
    if len(df) > 0:
        df = df.sort_values("timestamp_seconds").reset_index(drop=True)
        df["goal_number"] = range(1, len(df) + 1)

    # Create metadata
    if len(df) > 0:
        total_goals = len(df)
        goals_by_type = df["goal_type"].value_counts().to_dict()
        goals_by_team = df["team_name"].value_counts().to_dict()

        # Calculate goals per period
        goals_by_period = df["period"].value_counts().to_dict()

        context.add_output_metadata(
            {
                "total_goals": total_goals,
                "goals_by_type": MetadataValue.json(goals_by_type),
                "goals_by_team": MetadataValue.json(goals_by_team),
                "goals_by_period": MetadataValue.json(goals_by_period),
                "first_goal_minute": (
                    int(df.iloc[0]["minute"]) if total_goals > 0 else None
                ),
                "last_goal_minute": (
                    int(df.iloc[-1]["minute"]) if total_goals > 0 else None
                ),
                "avg_goal_minute": float(round(df["minute"].mean(), 2)),
                "preview": MetadataValue.md(df.to_markdown(index=False)),
            }
        )
    else:
        context.add_output_metadata(
            {"total_goals": 0, "message": "No goals scored in this match"}
        )

    output_dir = context.resources.football_config.output_dir
    os.makedirs(output_dir, exist_ok=True)
    df.to_csv(os.path.join(output_dir, "2d_get_goal_minutes.csv"), index=False)

    return df


@asset(
    description="Gets the first foul committed in the second half of the game",
    group_name="analysis",
    compute_kind="python",
    required_resource_keys={"football_config"},
)
def get_first_foul_in_second_half(
    context: AssetExecutionContext, validated_events: List[Dict[str, Any]]
) -> pd.DataFrame:
    """
    Identifies the time (in seconds from the beginning of the match) when the first foul was
    committed in the second half.

    Args:
        validated_events: List of event dictionaries

    Returns:
        DataFrame containing foul event data.
    """

    first_foul = None

    # Look for the first foul in the second half (period 2)
    for event in validated_events:
        event_type = event.get("type", {}).get("name", "")
        period = event.get("period")

        # Check if this is a foul in the second half
        if event_type == "Foul Committed" and period == 2:
            timestamp = event.get("timestamp", "00:00:00.000")
            timestamp_seconds = parse_timestamp_to_seconds(timestamp)

            player_id = event.get("player", {}).get("id")
            player_name = event.get("player", {}).get("name", "Unknown")
            team_name = event.get("team", {}).get("name", "Unknown")
            minute = event.get("minute")
            second = event.get("second")

            # Get foul context
            foul_type = event.get("foul_committed", {}).get("type", {}).get("name")
            card = event.get("foul_committed", {}).get("card", {}).get("name")

            first_foul = {
                "player_id": player_id,
                "player_name": player_name,
                "team_name": team_name,
                "period": period,
                "minute": minute,
                "second": second,
                "timestamp": timestamp,
                "timestamp_seconds": float(round(timestamp_seconds, 3)),
                "match_time": (
                    f"{minute}:{second:02d}"
                    if minute is not None and second is not None
                    else "Unknown"
                ),
                "foul_type": foul_type,
                "card": card,
            }

            # Break after finding the first foul
            break

    # Create DataFrame
    if first_foul:
        df = pd.DataFrame([first_foul])

        context.add_output_metadata(
            {
                "foul_found": True,
                "player": first_foul["player_name"],
                "team": first_foul["team_name"],
                "timestamp_seconds": first_foul["timestamp_seconds"],
                "match_time": first_foul["match_time"],
                "card_given": first_foul["card"] if first_foul["card"] else "No card",
                "preview": MetadataValue.md(df.to_markdown(index=False)),
            }
        )
    else:
        # No foul found in second half
        df = pd.DataFrame()

        context.add_output_metadata(
            {
                "foul_found": False,
                "message": "No fouls committed in the second half",
            }
        )

    output_dir = context.resources.football_config.output_dir
    os.makedirs(output_dir, exist_ok=True)
    df.to_csv(os.path.join(output_dir, "2e_first_foul_in_second_half.csv"), index=False)

    return df
