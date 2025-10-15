from dagster import Definitions
from src.matchday_pipeline.defs.assets.ingestion import raw_events_data, raw_lineup_data
from src.matchday_pipeline.defs.assets.validation import (
    validated_events,
    validated_lineup,
)
from src.matchday_pipeline.defs.assets.analysis import (
    player_time_on_pitch,
    calculate_match_duration,
    total_passes_per_player,
    get_goal_minutes,
    get_first_foul_in_second_half,
)
from src.matchday_pipeline.defs.assets.load_db import load_match_data
from src.matchday_pipeline.defs.resources import PostgresResource
from src.matchday_pipeline.defs.assets.sql_queries import (
    furthest_shot_from_goal,
    penalty_box_events,
    longest_shot_gap,
)
from src.matchday_pipeline.defs.jobs.pipelines import full_pipeline_job
from src.matchday_pipeline.defs.checks.asset_checks import (
    check_first_foul_second_half,
    check_goal_minutes,
    check_match_duration_positive,
    check_pass_distribution,
    check_player_time_on_pitch,
)
from src.matchday_pipeline.defs.config import FootballDataConfig

defs = Definitions(
    assets=[
        raw_lineup_data,
        raw_events_data,
        validated_lineup,
        validated_events,
        player_time_on_pitch,
        calculate_match_duration,
        total_passes_per_player,
        get_goal_minutes,
        get_first_foul_in_second_half,
        load_match_data,
        furthest_shot_from_goal,
        penalty_box_events,
        longest_shot_gap,
    ],
    asset_checks=[
        check_first_foul_second_half,
        check_goal_minutes,
        check_match_duration_positive,
        check_pass_distribution,
        check_player_time_on_pitch,
    ],
    jobs=[full_pipeline_job],
    resources={
        # Ideally the values here should be loaded from environment variables or secrets manager.
        "postgres": PostgresResource(
            host="localhost",
            database="tottenham_analytics_db",
            user="analytics_user",
            password="analytics_password",
        ),
        "football_config": FootballDataConfig(),
    },
)
