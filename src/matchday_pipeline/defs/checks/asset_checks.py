from dagster import asset_check, AssetCheckResult, AssetCheckSeverity
import pandas as pd


@asset_check(
    asset="player_time_on_pitch",
    description="Check that all players have positive time on pitch",
)
def check_player_time_on_pitch(player_time_on_pitch: pd.DataFrame) -> AssetCheckResult:
    if player_time_on_pitch.empty:
        return AssetCheckResult(
            passed=False,
            description="No player time data found",
            severity=AssetCheckSeverity.ERROR,
        )

    negative_time = (player_time_on_pitch["seconds_on_pitch"] < 0).sum()
    passed = negative_time == 0

    return AssetCheckResult(
        passed=bool(passed),
        description=(
            f"{negative_time} players have negative time on pitch"
            if not passed
            else "All players have valid time on pitch"
        ),
        metadata={
            "total_players": len(player_time_on_pitch),
            "negative_time_players": int(negative_time),
        },
        severity=AssetCheckSeverity.ERROR if not passed else AssetCheckSeverity.WARN,
    )


@asset_check(
    asset="player_time_on_pitch",
    description="Check average time on pitch is reasonable",
)
def check_avg_time_on_pitch(player_time_on_pitch: pd.DataFrame) -> AssetCheckResult:
    if player_time_on_pitch.empty:
        return AssetCheckResult(
            passed=False,
            description="No player time data",
            severity=AssetCheckSeverity.ERROR,
        )

    avg_time = player_time_on_pitch["seconds_on_pitch"].mean()
    passed = 0 < avg_time <= 5400  # max 90 minutes

    return AssetCheckResult(
        passed=bool(passed),
        description=f"Average time on pitch: {avg_time:.2f} seconds",
        metadata={"avg_time_on_pitch_seconds": float(avg_time)},
        severity=AssetCheckSeverity.ERROR if not passed else AssetCheckSeverity.WARN,
    )


@asset_check(
    asset="calculate_match_duration",
    description="Check that match duration is positive",
)
def check_match_duration_positive(
    calculate_match_duration: pd.DataFrame,
) -> AssetCheckResult:
    if calculate_match_duration.empty:
        return AssetCheckResult(
            passed=False,
            description="No match duration data",
            severity=AssetCheckSeverity.ERROR,
        )

    duration = calculate_match_duration.iloc[0]["total_match_duration_seconds"]
    passed = duration > 0

    return AssetCheckResult(
        passed=bool(passed),
        description=f"Match duration: {duration} seconds",
        metadata={"match_duration_seconds": float(duration)},
        severity=AssetCheckSeverity.ERROR if not passed else AssetCheckSeverity.WARN,
    )


@asset_check(
    asset="total_passes_per_player",
    description="Check that passes are distributed among multiple players",
)
def check_pass_distribution(total_passes_per_player: pd.DataFrame) -> AssetCheckResult:
    if total_passes_per_player.empty:
        return AssetCheckResult(
            passed=False,
            description="No passes found in match",
            severity=AssetCheckSeverity.ERROR,
        )

    total_passes = total_passes_per_player["total_passes"].sum()
    max_passes = total_passes_per_player["total_passes"].max()
    passed = max_passes / total_passes < 0.5  # no single player dominates >50%

    return AssetCheckResult(
        passed=bool(passed),
        description=f"Max passes by single player: {max_passes} ({max_passes/total_passes*100:.1f}%)",
        metadata={"total_passes": int(total_passes), "max_passes": int(max_passes)},
        severity=AssetCheckSeverity.WARN if not passed else AssetCheckSeverity.WARN,
    )


@asset_check(asset="get_goal_minutes", description="Check that goal minutes are valid")
def check_goal_minutes(get_goal_minutes: pd.DataFrame) -> AssetCheckResult:
    if get_goal_minutes.empty:
        return AssetCheckResult(
            passed=True,
            description="No goals scored in match",
            severity=AssetCheckSeverity.WARN,
        )

    invalid_minutes = (
        (get_goal_minutes["minute"] < 0) | (get_goal_minutes["minute"] > 120)
    ).sum()
    passed = invalid_minutes == 0

    return AssetCheckResult(
        passed=bool(passed),
        description=(
            f"{invalid_minutes} goals have invalid minute values"
            if invalid_minutes > 0
            else "All goals have valid minute values"
        ),
        metadata={
            "total_goals": len(get_goal_minutes),
            "invalid_minutes": int(invalid_minutes),
        },
        severity=AssetCheckSeverity.ERROR if not passed else AssetCheckSeverity.WARN,
    )


@asset_check(
    asset="get_first_foul_in_second_half",
    description="Check that foul timestamp is reasonable",
)
def check_first_foul_second_half(
    get_first_foul_in_second_half: pd.DataFrame,
) -> AssetCheckResult:
    if get_first_foul_in_second_half.empty:
        return AssetCheckResult(
            passed=True,
            description="No fouls committed in second half",
            severity=AssetCheckSeverity.WARN,
        )

    foul_time = get_first_foul_in_second_half.iloc[0]["timestamp_seconds"]
    passed = foul_time >= 0

    return AssetCheckResult(
        passed=bool(passed),
        description=f"First foul in second half at {foul_time} seconds",
        metadata={"timestamp_seconds": float(foul_time)},
        severity=AssetCheckSeverity.ERROR if not passed else AssetCheckSeverity.WARN,
    )
