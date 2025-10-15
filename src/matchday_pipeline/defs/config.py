import os
from dagster import Config
from pydantic import Field


class FootballDataConfig(Config):
    """Configuration for the matchday pipeline"""

    project_root: str = Field(
        default_factory=lambda: os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../..")
        ),
        description="Absolute path to the project root directory.",
    )

    lineup_file_path: str = Field(
        default="data/lineups_*.json",
        description="Path to the raw lineup json file.",
    )

    events_file_path: str = Field(
        default="data/events_*.json",
        description="Path to the raw events json file.",
    )

    output_dir: str = Field(
        default_factory=lambda: os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../..", "output")
        ),
        description="Directory for output files",
    )

    db_connection_string: str = Field(
        default="", description="Database connection string"
    )
