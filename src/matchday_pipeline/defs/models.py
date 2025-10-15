from typing import List, Dict, Any


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

        for field in DataValidator.LINEUP_REQUIRED_FIELDS:
            if field not in data:
                missing.append(field)

        # Validating that each player has an id
        if "lineup" in data and isinstance(data["lineup"], list):
            for idx, player in enumerate(data["lineup"]):
                if "player_id" not in player:
                    warnings.append(f"Player at index {idx} missing player_id")

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

        # Check first event has required fields
        if data:
            first_event = data[0]
            for field in DataValidator.EVENTS_REQUIRED_FIELDS:
                if field not in first_event:
                    missing.append(field)

        # Check for duplicate event IDs
        event_ids = [e.get("id") for e in data if "id" in e]
        if len(event_ids) != len(set(event_ids)):
            warnings.append("Duplicate event IDs found")

        is_valid = len(missing) == 0 and len(errors) == 0

        return ValidationResult(is_valid, missing, errors, warnings, len(data))
