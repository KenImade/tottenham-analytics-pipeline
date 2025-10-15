from dagster import define_asset_job

full_pipeline_job = define_asset_job(
    name="full_matchday_analysis",
    description="Run complete matchday analysis pipeline",
    selection=[
        "raw_lineup_data",
        "raw_events_data",
        "validated_lineup",
        "validated_events",
        "player_time_on_pitch",
        "calculate_match_duration",
        "total_passes_per_player",
        "get_goal_minutes",
        "get_first_foul_in_second_half",
        "load_match_data",
        "furthest_shot_from_goal",
        "penalty_box_events",
        "longest_shot_gap",
    ],
)
