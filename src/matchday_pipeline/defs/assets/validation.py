from typing import Dict, Any, List, Tuple
from dagster import asset, AssetExecutionContext, MetadataValue
from matchday_pipeline.defs.assets.ingestion import raw_lineup_data, raw_events_data
from matchday_pipeline.defs.models import DataValidator


@asset(
    description="Validated lineup data with quality checks and deduplication",
    group_name="validation",
    deps=[raw_lineup_data],
)
def validated_lineup(
    context: AssetExecutionContext, raw_lineup_data: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Validate lineup data and return if valid"""

    # Handle both single team and multiple teams
    teams_data = (
        raw_lineup_data if isinstance(raw_lineup_data, list) else [raw_lineup_data]
    )

    all_valid = True
    validation_results = []
    total_duplicates_removed = 0
    deduplicated_teams = []

    for idx, team_data in enumerate(teams_data):
        # Validate
        result = DataValidator.validate_lineup(team_data)
        validation_results.append(result)

        if not result.is_valid:
            all_valid = False
            context.log.error(f"Team {idx} validation failed: {result.missing_fields}")

        if result.warnings:
            for warning in result.warnings:
                context.log.warning(f"Team {idx}: {warning}")

        # Deduplicate players
        deduplicated_team, duplicates_removed = (
            DataValidator.deduplicate_lineup_players(team_data)
        )
        deduplicated_teams.append(deduplicated_team)
        total_duplicates_removed += duplicates_removed

        if duplicates_removed > 0:
            context.log.info(
                f"Team {idx} ({team_data.get('team_name', 'Unknown')}): "
                f"Removed {duplicates_removed} duplicate player(s)"
            )

    if not all_valid:
        raise ValueError("Lineup validation failed - check logs for details")

    context.add_output_metadata(
        {
            "teams_count": len(teams_data),
            "total_players": sum(r.record_count for r in validation_results),
            "duplicates_removed": total_duplicates_removed,
            "validation_status": "PASSED",
        }
    )

    # Return deduplicated data or original based on whether duplicates were found
    return (
        deduplicated_teams
        if isinstance(raw_lineup_data, list)
        else deduplicated_teams[0]
    )


@asset(
    description="Validated events data with quality checks and deduplication",
    group_name="validation",
    deps=[raw_events_data],
)
def validated_events(
    context: AssetExecutionContext, raw_events_data: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Validate events data and return if valid"""

    # Validate
    result = DataValidator.validate_events(raw_events_data)

    if not result.is_valid:
        context.log.error(f"Validation failed: Missing fields: {result.missing_fields}")
        context.log.error(f"Errors: {result.errors}")
        raise ValueError("Events validation failed - check logs for details")

    if result.warnings:
        for warning in result.warnings:
            context.log.warning(warning)

    # Deduplicate events
    deduplicated_events, duplicates_removed = DataValidator.deduplicate_events(
        raw_events_data
    )

    if duplicates_removed > 0:
        context.log.info(f"Removed {duplicates_removed} duplicate event(s)")

    # Calculate event type distribution
    event_types = {}
    for event in deduplicated_events:
        event_type = event.get("type", {}).get("name", "Unknown")
        event_types[event_type] = event_types.get(event_type, 0) + 1

    context.add_output_metadata(
        {
            "event_count": result.record_count,
            "event_count_after_dedup": len(deduplicated_events),
            "duplicates_removed": duplicates_removed,
            "validation_status": "PASSED",
            "event_types": len(event_types),
            "top_events": MetadataValue.json(
                dict(sorted(event_types.items(), key=lambda x: x[1], reverse=True)[:5])
            ),
        }
    )

    return deduplicated_events
