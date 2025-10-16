"""Firestore tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_firestore.streams import FirestoreStream


class TapFirestore(Tap):
    """Firestore tap class."""

    name = "tap-firestore"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "credentials_path",
            th.StringType,
            description="Path to the Firebase credentials JSON file",
        ),
        th.Property(
            "credentials_json",
            th.StringType,
            description="Firebase credentials as JSON string (alternative to credentials_path)",
        ),
        th.Property(
            "credentials_base64",
            th.StringType,
            description="Firebase credentials as base64-encoded JSON string (alternative to credentials_path)",
        ),
        th.Property(
            "project_id",
            th.StringType,
            required=True,
            description="GCP Project ID",
        ),
        th.Property(
            "collections",
            th.ArrayType(
                th.ObjectType(
                    th.Property(
                        "name",
                        th.StringType,
                        required=True,
                        description="Firestore collection name",
                    ),
                    th.Property(
                        "replication_key",
                        th.StringType,
                        description="Field to use for incremental replication (e.g., 'updated_at', 'created_at')",
                    ),
                    th.Property(
                        "replication_key_type",
                        th.StringType,
                        description="Type of replication key: 'timestamp' or 'string' (default: 'timestamp')",
                    ),
                    th.Property(
                        "limit",
                        th.IntegerType,
                        description="Maximum number of documents to extract per sync (optional, for testing or rate limiting)",
                    ),
                )
            ),
            required=True,
            description="List of collections to extract",
        ),
    ).to_dict()

    def discover_streams(self):
        """Return a list of discovered streams."""
        collections = self.config.get("collections", [])

        for collection_config in collections:
            yield FirestoreStream(
                tap=self,
                name=collection_config["name"],
                replication_key=collection_config.get("replication_key"),
                replication_key_type=collection_config.get("replication_key_type", "timestamp"),
                limit=collection_config.get("limit"),
            )


if __name__ == "__main__":
    TapFirestore.cli()
