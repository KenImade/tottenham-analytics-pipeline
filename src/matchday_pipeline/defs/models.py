from typing import List, Dict, Any, Tuple


class ValidationResult:
    """Result of data validation"""

    def __init__(
        self,
        is_valid: bool,
        missing_fields: List[str],
        errors: List[str],
        warnings: List[str],
        record_count: int,
    ):

        self.is_valid = is_valid
        self.missing_fields = missing_fields
        self.errors = errors
        self.warnings = warnings
        self.record_count = record_count


class DataValidator:
    """Validates football match data"""

    LINEUP_REQUIRED_FIELDS = [
        "match_date",
        "match_id",
        "events",
        "formations",
        "lineup",
        "team_id",
        "team_name",
    ]

    EVENTS_REQUIRED_FIELDS = ["match_id", "id", "index", "period", "timestamp", "type"]

    @staticmethod
    def validate_lineup(data: Dict[str, Any]) -> ValidationResult:
        """Validate lineup data structure"""
        missing = []
        errors = []
        warnings = []

        # Check for missing required fields
        for field in DataValidator.LINEUP_REQUIRED_FIELDS:
            if field not in data:
                missing.append(field)

        # Validate match_date
        if "match_date" in data:
            match_date = data.get("match_date")
            if not isinstance(match_date, str) or not match_date:
                warnings.append(f"Invalid 'match_date': {match_date}")
            # Could add date format validation here

        # Validate match_id
        if "match_id" in data:
            match_id = data.get("match_id")
            if not isinstance(match_id, str):
                warnings.append(f"Invalid 'match_id': {match_id}")

        # Validate events
        if "events" in data:
            events = data.get("events")
            if not isinstance(events, list):
                warnings.append(f"'events' should be a list, got: {type(events)}")
            elif not events:
                warnings.append("'events' list is empty")

        # Validate formations
        if "formations" in data:
            formations = data.get("formations")
            if not isinstance(formations, list):
                warnings.append(
                    f"'formations' should be a list, got: {type(formations)}"
                )
            elif not formations:
                warnings.append("'formations' list is empty")

        # Validate team_id
        if "team_id" in data:
            team_id = data.get("team_id")
            if not isinstance(team_id, int):
                warnings.append(f"Non-integer 'team_id': {team_id}")
            elif team_id <= 0:
                warnings.append(f"Invalid 'team_id': {team_id}")

        # Validate team_name
        if "team_name" in data:
            team_name = data.get("team_name")
            if not isinstance(team_name, str) or not team_name:
                warnings.append(f"Invalid 'team_name': {team_name}")

        is_valid = len(missing) == 0 and len(errors) == 0
        record_count = len(data.get("lineup", []))

        return ValidationResult(is_valid, missing, errors, warnings, record_count)

    @staticmethod
    def validate_events(data: List[Dict[str, Any]]) -> ValidationResult:
        """Validate events data structure"""
        missing = []
        errors = []
        warnings = []

        if not isinstance(data, list):
            errors.append("Events data must be a list")
            return ValidationResult(False, missing, errors, warnings, 0)

        # Check each event has required fields and validate their values
        for idx, event in enumerate(data):
            if not isinstance(event, dict):
                errors.append(f"Event at index {idx} is not a dictionary")
                continue

            # Check for missing required fields
            for field in DataValidator.EVENTS_REQUIRED_FIELDS:
                if field not in event:
                    missing.append(f"Event at index {idx} missing field: '{field}'")

            # Validate match_id
            if "match_id" in event:
                match_id = event.get("match_id")
                if not isinstance(match_id, str):
                    warnings.append(
                        f"Event at index {idx} has invalid 'match_id': {match_id}"
                    )

            # Validate id
            if "id" in event:
                event_id = event.get("id")
                if not isinstance(event_id, str) or not event_id:
                    warnings.append(
                        f"Event at index {idx} has invalid 'id': {event_id}"
                    )

            # Validate index
            if "index" in event:
                event_index = event.get("index")
                if not isinstance(event_index, int):
                    warnings.append(
                        f"Event at index {idx} has non-integer 'index' value: {event_index}"
                    )
                elif event_index < 0:
                    warnings.append(
                        f"Event at index {idx} has negative 'index' value: {event_index}"
                    )

            # Validate period
            if "period" in event:
                period = event.get("period")
                if not isinstance(period, int):
                    warnings.append(
                        f"Event at index {idx} has non-integer 'period': {period}"
                    )
                elif period not in [1, 2, 3, 4, 5]:  # Regular time, extra time periods
                    warnings.append(
                        f"Event at index {idx} has unusual 'period' value: {period}"
                    )

            # Validate timestamp
            if "timestamp" in event:
                timestamp = event.get("timestamp")
                if not isinstance(timestamp, str):
                    warnings.append(
                        f"Event at index {idx} has non-string 'timestamp': {timestamp}"
                    )
                elif not timestamp:
                    warnings.append(f"Event at index {idx} has empty 'timestamp'")
                else:
                    # Check timestamp format (HH:MM:SS.mmm)
                    parts = timestamp.split(":")
                    if len(parts) != 3:
                        warnings.append(
                            f"Event at index {idx} has malformed 'timestamp': {timestamp}"
                        )

            # Validate type
            if "type" in event:
                event_type = event.get("type")
                if not isinstance(event_type, dict):
                    warnings.append(
                        f"Event at index {idx} has non-dict 'type': {event_type}"
                    )
                elif "id" not in event_type or "name" not in event_type:
                    warnings.append(
                        f"Event at index {idx} has incomplete 'type' structure (missing 'id' or 'name')"
                    )

        # Check for duplicate event IDs with detailed reporting
        event_ids = {}
        for idx, event in enumerate(data):
            if "id" in event:
                event_id = event.get("id")
                if event_id in event_ids:
                    event_ids[event_id].append(idx)
                else:
                    event_ids[event_id] = [idx]

        # Report duplicates with details
        duplicates = {
            eid: indices for eid, indices in event_ids.items() if len(indices) > 1
        }
        if duplicates:
            for event_id, indices in duplicates.items():
                warnings.append(
                    f"Duplicate event ID '{event_id}' found at indices: {indices} "
                    f"({len(indices)} occurrences)"
                )

        is_valid = len(missing) == 0 and len(errors) == 0

        return ValidationResult(is_valid, missing, errors, warnings, len(data))

    @staticmethod
    def deduplicate_events(
        data: List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Remove duplicate events, keeping first occurrence

        Returns:
            Tuple of (deduplicated_data, number_of_duplicates_removed)
        """
        seen_ids = set()
        deduplicated = []
        duplicates_removed = 0

        for event in data:
            event_id = event.get("id")
            if event_id:
                if event_id not in seen_ids:
                    seen_ids.add(event_id)
                    deduplicated.append(event)
                else:
                    duplicates_removed += 1
            else:
                deduplicated.append(event)

        return deduplicated, duplicates_removed

    @staticmethod
    def deduplicate_lineup_players(data: Dict[str, Any]) -> Tuple[Dict[str, Any], int]:
        """Remove duplicate players from lineup, keeping first occurrence

        Returns:
            Tuple of (deduplicated_data, number_of_duplicates_removed)
        """
        if "lineup" not in data or not isinstance(data["lineup"], list):
            return data, 0

        seen_ids = set()
        deduplicated_players = []
        duplicates_removed = 0

        for player in data["lineup"]:
            player_id = player.get("player_id")
            if player_id:
                if player_id not in seen_ids:
                    seen_ids.add(player_id)
                    deduplicated_players.append(player)
                else:
                    duplicates_removed += 1
            else:
                deduplicated_players.append(player)

        # Create new dict with deduplicated lineup
        result = data.copy()
        result["lineup"] = deduplicated_players

        return result, duplicates_removed
