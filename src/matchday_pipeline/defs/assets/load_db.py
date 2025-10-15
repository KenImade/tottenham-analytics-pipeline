from dagster import asset, AssetExecutionContext, MetadataValue
from src.matchday_pipeline.defs.resources import PostgresResource
from typing import List, Dict, Any


@asset(
    description="Loads match data into postgres database",
    group_name="loading",
    compute_kind="sql",
)
def load_match_data(
    context: AssetExecutionContext,
    postgres: PostgresResource,
    validated_events: List[Dict[str, Any]],
):
    """
    Loads match data into a postgres database.

    Args:
        postgres: Database connection
        context: Dagster context
        validated_events: List of dictionaries containing match event data.
    """
    conn = postgres.get_connection()
    cursor = conn.cursor()

    loaded_count = 0
    error_count = 0
    errors = []

    try:
        # Create table if it does not exist
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                event_id VARCHAR(100) PRIMARY KEY,
                match_id VARCHAR(50),
                team_id INTEGER,
                team_name VARCHAR(250),
                player_id INTEGER,
                player_name VARCHAR(250),
                event_type VARCHAR(100),
                timestamp VARCHAR(20),
                location_x FLOAT,
                location_y FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        context.log.info(
            f"Table 'events' ready. Processing {len(validated_events)} events."
        )

        # Prepare insert statement
        insert_query = """
            INSERT INTO events (
                event_id, match_id, team_id, team_name, player_id, player_name,
                event_type, timestamp, location_x, location_y
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                match_id = EXCLUDED.match_id,
                team_id = EXCLUDED.team_id,
                team_name = EXCLUDED.team_name,
                player_id = EXCLUDED.player_id,
                player_name = EXCLUDED.player_name,
                event_type = EXCLUDED.event_type,
                timestamp = EXCLUDED.timestamp,
                location_x = EXCLUDED.location_x,
                location_y = EXCLUDED.location_y
        """

        # Load data into table
        for event in validated_events:
            try:
                # Extract event data
                event_id = event.get("id")
                match_id = event.get("match_id")
                team_id = event.get("team", {}).get("id")
                team_name = event.get("team", {}).get("name")
                player_id = event.get("player", {}).get("id")
                player_name = event.get("player", {}).get("name")
                event_type = event.get("type", {}).get("name")
                timestamp = event.get("timestamp")

                # Extract location if available
                location = event.get("location")
                location_x = None
                location_y = None

                if location and isinstance(location, (list, tuple)):
                    location_x = location[0]
                    location_y = location[1]

                # Execute insert
                cursor.execute(
                    insert_query,
                    (
                        event_id,
                        match_id,
                        team_id,
                        team_name,
                        player_id,
                        player_name,
                        event_type,
                        timestamp,
                        location_x,
                        location_y,
                    ),
                )

                loaded_count += 1

            except Exception as e:
                error_count += 1
                error_msg = (
                    f"Error loading event {event.get('id', 'unknown')}: {str(e)}"
                )
                errors.append(error_msg)
                context.log.warning(error_msg)
                continue

        # Commit the transaction
        conn.commit()
        context.log.info(f"Successfully loaded {loaded_count} events to database.")

        # Add output metadata
        context.add_output_metadata(
            {
                "total_events_processed": len(validated_events),
                "events_loaded": loaded_count,
                "events_failed": error_count,
                "success_rate": (
                    f"{(loaded_count / len(validated_events) * 100):.2f}%"
                    if len(validated_events) > 0
                    else "0%"
                ),
                "table_name": "events",
                "errors": (
                    MetadataValue.md("\n".join(errors[:10]) if errors else "No errors")
                    if errors
                    else None
                ),
            }
        )

    except Exception as e:
        conn.rollback()
        error_msg = f"Failed to load match data: {str(e)}"
        context.log.error(error_msg)

        context.add_output_metadata(
            {
                "status": "failed",
                "error": str(e),
                "events_loaded": loaded_count,
            }
        )

        raise

    finally:
        cursor.close()
        conn.close()
