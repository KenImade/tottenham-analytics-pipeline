import json
from pathlib import Path
from dagster import asset, AssetExecutionContext, MetadataValue
from src.matchday_pipeline.defs.config import FootballDataConfig


@asset(
    description="Load lineup data from JSON file",
    group_name="ingestion",
    compute_kind="json",
)
def raw_lineup_data(context: AssetExecutionContext, config: FootballDataConfig):
    """Load and return new lineup data"""
    base = Path(config.project_root)
    pattern = config.lineup_file_path

    files = list(base.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No lineup file found for pattern: {base / pattern}")

    file_path = files[0]
    context.log.info(f"Loading lineup data from {file_path}")

    data = []
    with open(file_path, "r", encoding="utf-8-sig") as f:
        for line in f:
            if line.strip():
                data.append(json.loads(line))

    context.add_output_metadata(
        {
            "file_path": str(file_path),
            "file_size_bytes": file_path.stat().st_size,
            "teams": len(data) if isinstance(data, list) else 1,
            "preview": MetadataValue.json(
                data[0] if isinstance(data, list) and data else data
            ),
        }
    )

    return data


@asset(
    description="Load events data from JSON file",
    group_name="ingestion",
    compute_kind="json",
)
def raw_events_data(context: AssetExecutionContext, config: FootballDataConfig):
    """Load and return raw events data"""
    base = Path(config.project_root)
    pattern = config.events_file_path

    files = list(base.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No events file found for pattern: {base / pattern}")

    file_path = files[0]
    context.log.info(f"Loading events data from {file_path}")

    data = []
    with open(file_path, "r", encoding="utf-8-sig") as f:
        for line in f:
            if line.strip():
                data.append(json.loads(line))

    context.add_output_metadata(
        {
            "file_path": str(file_path),
            "file_size_bytes": file_path.stat().st_size,
            "event_count": len(data) if isinstance(data, list) else 0,
            "sample_event": MetadataValue.json(
                data[0] if isinstance(data, list) and data else {}
            ),
        }
    )

    return data
