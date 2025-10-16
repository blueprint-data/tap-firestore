"""Stream type classes for tap-firestore."""

from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from google.cloud import firestore
from google.oauth2 import service_account
from singer_sdk import typing as th
from singer_sdk.streams import Stream


class FirestoreStream(Stream):
    """Stream class for Firestore collections."""

    def __init__(
        self,
        tap,
        name: str,
        replication_key: Optional[str] = None,
        replication_key_type: str = "timestamp",
        limit: Optional[int] = None,
    ):
        """Initialize the stream.

        Args:
            tap: The tap instance.
            name: The collection name.
            replication_key: Field to use for incremental replication.
            replication_key_type: Type of replication key ('timestamp' or 'string').
            limit: Maximum number of documents to extract per sync.
        """
        self.collection_name = name
        self._replication_key = replication_key
        self.replication_key_type = replication_key_type
        self.limit = limit
        super().__init__(tap=tap, name=name, schema=None)

    @property
    def replication_key(self) -> Optional[str]:
        """Return replication key."""
        return self._replication_key

    @property
    def schema(self) -> dict:
        """Return dynamic schema.

        Since Firestore is schemaless, we define a flexible schema that includes
        common fields and allows additional properties.
        """
        properties = {
            "_id": th.StringType,
            "_sdc_extracted_at": th.DateTimeType,
        }

        # Add replication key to schema if defined
        if self.replication_key:
            if self.replication_key_type == "timestamp":
                properties[self.replication_key] = th.DateTimeType
            else:
                properties[self.replication_key] = th.StringType

        schema_dict = th.PropertiesList(
            *[th.Property(k, v) for k, v in properties.items()]
        ).to_dict()

        # Allow additional properties since Firestore is schemaless
        schema_dict["additionalProperties"] = True

        return schema_dict

    def _normalize_credentials(self, creds_dict: dict) -> dict:
        """Normalize credentials dictionary to fix common issues.

        Args:
            creds_dict: The credentials dictionary.

        Returns:
            Normalized credentials dictionary.
        """
        # Fix private_key newlines - common issue with environment variables
        if "private_key" in creds_dict:
            private_key = creds_dict["private_key"]
            # Replace literal \n strings with actual newlines
            if "\\n" in private_key:
                creds_dict["private_key"] = private_key.replace("\\n", "\n")

        return creds_dict

    def _get_firestore_client(self) -> firestore.Client:
        """Create and return a Firestore client."""
        credentials_path = self.config.get("credentials_path")
        credentials_json = self.config.get("credentials_json")
        credentials_base64 = self.config.get("credentials_base64")
        project_id = self.config["project_id"]

        try:
            # Define required scopes for Firestore
            scopes = [
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/datastore",
            ]

            if credentials_base64:
                # Decode base64 string and parse JSON
                self.logger.info("Using base64-encoded credentials")
                decoded_json = base64.b64decode(credentials_base64).decode("utf-8")
                creds_dict = json.loads(decoded_json)
                creds_dict = self._normalize_credentials(creds_dict)
                credentials = service_account.Credentials.from_service_account_info(
                    creds_dict, scopes=scopes
                )
            elif credentials_json:
                # Parse JSON string
                self.logger.info("Using JSON string credentials")
                creds_dict = json.loads(credentials_json)
                creds_dict = self._normalize_credentials(creds_dict)
                credentials = service_account.Credentials.from_service_account_info(
                    creds_dict, scopes=scopes
                )
            elif credentials_path:
                # Load from file
                self.logger.info(f"Using credentials from file: {credentials_path}")
                credentials = service_account.Credentials.from_service_account_file(
                    credentials_path, scopes=scopes
                )
            else:
                # Use default credentials
                self.logger.info("Using default credentials")
                credentials = None

            return firestore.Client(project=project_id, credentials=credentials)
        except Exception as e:
            self.logger.error(f"Error creating Firestore client: {e}")
            raise

    def _convert_value(self, value: Any) -> Any:
        """Convert Firestore values to JSON-serializable types.

        Args:
            value: The value to convert.

        Returns:
            JSON-serializable value.
        """
        if isinstance(value, firestore.SERVER_TIMESTAMP.__class__):
            return None
        elif hasattr(value, "isoformat"):  # datetime objects
            return value.isoformat()
        elif isinstance(value, dict):
            return {k: self._convert_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [self._convert_value(item) for item in value]
        elif isinstance(value, bytes):
            return value.decode("utf-8", errors="ignore")
        return value

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return a generator of record-type dictionary objects.

        Args:
            context: Stream context.

        Yields:
            One record at a time.
        """
        client = self._get_firestore_client()
        collection_ref = client.collection(self.collection_name)

        # Build query with incremental replication support
        query = collection_ref

        # Order by replication key if specified
        if self.replication_key:
            query = query.order_by(self.replication_key)

            starting_value = self.get_starting_replication_key_value(context)

            if starting_value:
                self.logger.info(
                    f"Resuming {self.collection_name} from "
                    f"{self.replication_key}: {starting_value}"
                )
                # Use start_after with a document snapshot dict
                # This is more efficient than where() for pagination
                query = query.start_after({self.replication_key: starting_value})

        # Apply limit if specified
        if self.limit:
            self.logger.info(f"Limiting query to {self.limit} documents")
            query = query.limit(self.limit)

        # Stream documents
        doc_count = 0
        for doc in query.stream():
            doc_dict = doc.to_dict()

            if doc_dict is None:
                continue

            # Convert Firestore-specific types
            record = {
                "_id": doc.id,
                "_sdc_extracted_at": self._get_current_timestamp(),
            }

            # Add all document fields
            for key, value in doc_dict.items():
                record[key] = self._convert_value(value)

            doc_count += 1
            yield record

        self.logger.info(f"Extracted {doc_count} documents from {self.collection_name}")

    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        from datetime import datetime, timezone

        return datetime.now(timezone.utc).isoformat()

    def get_starting_replication_key_value(
        self, context: Optional[dict]
    ) -> Optional[Any]:
        """Get the starting replication key value from state.

        Args:
            context: Stream context.

        Returns:
            Starting replication key value or None.
        """
        state = self.get_context_state(context)
        replication_key_value = state.get("replication_key_value")

        if replication_key_value and self.replication_key_type == "timestamp":
            # Convert ISO string back to datetime for Firestore query
            from datetime import datetime

            return datetime.fromisoformat(replication_key_value.replace("Z", "+00:00"))

        return replication_key_value
