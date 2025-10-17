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
        batch_size: int = 500,
        schema_config: Optional[dict] = None,
    ):
        """Initialize the stream.

        Args:
            tap: The tap instance.
            name: The collection name.
            replication_key: Field to use for incremental replication.
            replication_key_type: Type of replication key ('timestamp' or 'string').
            limit: Maximum number of documents to extract per sync.
            batch_size: Number of documents to fetch per batch (default: 500).
            schema_config: Optional schema configuration dict (field_name -> type_string).
        """
        self.collection_name = name
        self._replication_key = replication_key
        self.replication_key_type = replication_key_type
        self.limit = limit
        self.batch_size = batch_size
        self.schema_config = schema_config
        super().__init__(tap=tap, name=name, schema=None)

    @property
    def replication_key(self) -> Optional[str]:
        """Return replication key."""
        return self._replication_key

    def _infer_type(self, value: Any) -> th.JSONTypeHelper:
        """Infer Singer type from a Python value.

        Args:
            value: The value to infer type from.

        Returns:
            Singer type helper.
        """
        if value is None:
            return th.StringType  # Default to string for null values
        elif isinstance(value, bool):
            return th.BooleanType
        elif isinstance(value, int):
            return th.IntegerType
        elif isinstance(value, float):
            return th.NumberType
        elif hasattr(value, "isoformat"):  # datetime objects
            return th.DateTimeType
        elif isinstance(value, dict):
            return th.ObjectType()
        elif isinstance(value, list):
            if len(value) > 0:
                # Infer array item type from first element
                item_type = self._infer_type(value[0])
                return th.ArrayType(item_type)
            return th.ArrayType(th.StringType)  # Default array item type
        else:
            return th.StringType

    def _type_string_to_singer_type(self, type_str: str) -> th.JSONTypeHelper:
        """Convert a type string to a Singer type.

        Args:
            type_str: Type string (e.g., 'string', 'integer', 'datetime')

        Returns:
            Singer type helper.
        """
        type_map = {
            "string": th.StringType,
            "integer": th.IntegerType,
            "number": th.NumberType,
            "boolean": th.BooleanType,
            "datetime": th.DateTimeType,
            "object": th.ObjectType(),
            "array": th.ArrayType(th.StringType),
        }
        return type_map.get(type_str.lower(), th.StringType)

    def _build_schema_from_config(self) -> dict:
        """Build schema from provided configuration.

        Returns:
            Schema dictionary from config.
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

        # Add fields from schema config
        if self.schema_config:
            for field_name, field_type in self.schema_config.items():
                properties[field_name] = self._type_string_to_singer_type(field_type)

        schema_dict = th.PropertiesList(
            *[th.Property(k, v) for k, v in properties.items()]
        ).to_dict()

        # Allow additional properties since Firestore is schemaless
        schema_dict["additionalProperties"] = True

        return schema_dict

    def _discover_schema(self) -> dict:
        """Discover schema by sampling documents from the collection.

        Returns:
            Schema dictionary with discovered fields.
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

        # Sample documents to discover schema
        try:
            client = self._get_firestore_client()
            collection_ref = client.collection(self.collection_name)

            # Sample up to 10 documents to discover fields
            sample_size = 10
            sample_docs = list(collection_ref.limit(sample_size).stream())

            self.logger.info(
                f"Sampled {len(sample_docs)} documents from {self.collection_name} "
                "for schema discovery"
            )

            # Collect all field names and types from samples
            field_types = {}
            for doc in sample_docs:
                doc_dict = doc.to_dict()
                if doc_dict:
                    for key, value in doc_dict.items():
                        if key not in field_types:
                            field_types[key] = self._infer_type(value)

            # Add discovered fields to properties
            properties.update(field_types)

        except Exception as e:
            self.logger.warning(
                f"Could not discover schema for {self.collection_name}: {e}. "
                "Using minimal schema."
            )

        schema_dict = th.PropertiesList(
            *[th.Property(k, v) for k, v in properties.items()]
        ).to_dict()

        # Allow additional properties since Firestore is schemaless
        schema_dict["additionalProperties"] = True

        return schema_dict

    @property
    def schema(self) -> dict:
        """Return dynamic schema.

        If schema config is provided, use that. Otherwise, discover schema by sampling.
        """
        if not hasattr(self, "_schema_cache"):
            if self.schema_config:
                self.logger.info(
                    f"Using provided schema config for {self.collection_name}"
                )
                self._schema_cache = self._build_schema_from_config()
            else:
                self.logger.info(
                    f"Auto-discovering schema for {self.collection_name}"
                )
                self._schema_cache = self._discover_schema()
        return self._schema_cache

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
        if value is None:
            return None
        elif isinstance(value, firestore.SERVER_TIMESTAMP.__class__):
            return None
        elif hasattr(value, "isoformat"):  # datetime objects
            return value.isoformat()
        # Handle Firestore GeoPoint
        elif hasattr(value, "latitude") and hasattr(value, "longitude"):
            return {
                "latitude": value.latitude,
                "longitude": value.longitude
            }
        # Handle Firestore DocumentReference
        elif hasattr(value, "path") and hasattr(value, "id"):
            return value.path  # Convert reference to path string
        elif isinstance(value, dict):
            return {k: self._convert_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [self._convert_value(item) for item in value]
        elif isinstance(value, tuple):
            return [self._convert_value(item) for item in value]
        elif isinstance(value, bytes):
            return value.decode("utf-8", errors="ignore")
        # Catch any other special Firestore types
        elif hasattr(value, "__dict__") and not isinstance(value, (str, int, float, bool)):
            # Try to convert object to dict
            try:
                return {k: self._convert_value(v) for k, v in value.__dict__.items() if not k.startswith("_")}
            except:
                return str(value)
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

        # Build base query with incremental replication support
        base_query = collection_ref
        starting_value = None

        if self.replication_key:
            starting_value = self.get_starting_replication_key_value(context)

            if starting_value:
                self.logger.info(
                    f"Resuming {self.collection_name} where "
                    f"{self.replication_key} > {starting_value}"
                )
                # Use where() instead of order_by + start_after
                # This avoids requiring a composite index
                base_query = base_query.where(
                    filter=firestore.FieldFilter(
                        self.replication_key, ">", starting_value
                    )
                )

        # Paginate through documents to avoid timeouts
        total_extracted = 0
        last_doc = None

        self.logger.info(
            f"Starting extraction with batch_size={self.batch_size}"
            + (f", limit={self.limit}" if self.limit else "")
        )

        while True:
            # Build query for this batch
            query = base_query.order_by("__name__")  # Order by document ID (always indexed)

            if last_doc:
                query = query.start_after(last_doc)

            # Apply batch size
            batch_limit = self.batch_size
            if self.limit:
                remaining = self.limit - total_extracted
                if remaining <= 0:
                    break
                batch_limit = min(batch_limit, remaining)

            query = query.limit(batch_limit)

            # Fetch batch
            docs = list(query.stream())

            if not docs:
                # No more documents
                break

            self.logger.info(
                f"Processing batch of {len(docs)} documents "
                f"(total: {total_extracted + len(docs)})"
            )

            # Process documents in this batch
            for doc in docs:
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

                total_extracted += 1
                last_doc = doc
                yield record

            # If we got fewer docs than requested, we've reached the end
            if len(docs) < batch_limit:
                break

        self.logger.info(f"Extracted {total_extracted} documents from {self.collection_name}")

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
