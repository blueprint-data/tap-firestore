# tap-firestore

A [Singer](https://www.singer.io/) tap for extracting data from Google Cloud Firestore, built with the [Meltano Singer SDK](https://sdk.meltano.com).

## Features

- Extract data from multiple Firestore collections
- Incremental replication support using timestamp or string-based replication keys
- Flexible authentication (credentials file, JSON string, or base64-encoded string)
- Built with the Meltano Singer SDK for reliability and best practices

## Installation

### Using pip

```bash
pip install -e .
```

### Using Poetry

```bash
poetry install
```

## Configuration

### Required Settings

- `project_id`: Your GCP Project ID
- `collections`: Array of collection configurations to extract

### Optional Settings

- `credentials_path`: Path to Firebase credentials JSON file
- `credentials_json`: Firebase credentials as JSON string (alternative to credentials_path)
- `credentials_base64`: Firebase credentials as base64-encoded JSON string (alternative to credentials_path)

### Collection Configuration

Each collection in the `collections` array supports:

- `name` (required): Firestore collection name
- `replication_key` (optional): Field to use for incremental replication (e.g., `updated_at`, `created_at`)
- `replication_key_type` (optional): Type of replication key - `timestamp` (default) or `string`

## Usage

### Configuration File Example

Create a `config.json` file:

```json
{
  "project_id": "your-gcp-project-id",
  "credentials_path": "/path/to/credentials.json",
  "collections": [
    {
      "name": "users",
      "replication_key": "updated_at",
      "replication_key_type": "timestamp"
    },
    {
      "name": "orders",
      "replication_key": "created_at",
      "replication_key_type": "timestamp"
    },
    {
      "name": "products"
    }
  ]
}
```

### Using Environment Variables

Alternatively, use environment variables:

```bash
export TAP_FIRESTORE_PROJECT_ID="your-gcp-project-id"
export TAP_FIRESTORE_CREDENTIALS_PATH="/path/to/credentials.json"

# Or use base64-encoded credentials (useful for CI/CD)
export TAP_FIRESTORE_CREDENTIALS_BASE64="$(base64 -i credentials.json)"
```

For collections, create a config file or pass via JSON string.

### Base64 Credentials Example

To encode your credentials file:

```bash
# Encode the credentials file
base64 -i credentials.json

# Or on Linux
base64 -w 0 credentials.json
```

Then use it in your config:

```json
{
  "project_id": "your-gcp-project-id",
  "credentials_base64": "ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsC...",
  "collections": [...]
}
```

### Running the Tap

Discover available streams:

```bash
tap-firestore --config config.json --discover
```

Extract data:

```bash
tap-firestore --config config.json > output.json
```

With state file for incremental extraction:

```bash
tap-firestore --config config.json --state state.json > output.json
```

### Using with Meltano

1. Add to your `meltano.yml`:

```yaml
plugins:
  extractors:
  - name: tap-firestore
    namespace: tap_firestore
    pip_url: -e /path/to/tap-firestore
    config:
      project_id: your-gcp-project-id
      credentials_path: /path/to/credentials.json
      collections:
      - name: users
        replication_key: updated_at
        replication_key_type: timestamp
      - name: orders
        replication_key: created_at
        replication_key_type: timestamp
```

2. Run with Meltano:

```bash
meltano run tap-firestore target-jsonl
```

## Firebase Credentials Setup

1. Go to [Firebase Console](https://console.firebase.google.com/)
2. Select your project
3. Go to Project Settings > Service Accounts
4. Click "Generate New Private Key"
5. Save the JSON file securely
6. Use one of three methods:
   - **File path**: Set `credentials_path` to the file location
   - **JSON string**: Pass the JSON content directly in `credentials_json`
   - **Base64 encoded**: Encode the file and pass in `credentials_base64` (recommended for environment variables)

## Incremental Replication

The tap supports incremental replication by specifying a `replication_key` for each collection. The tap will:

1. Store the maximum value of the replication key in the state file
2. On subsequent runs, only extract documents where the replication key is greater than the stored value
3. Order results by the replication key for consistent extraction

### Timestamp-based Incremental

For timestamp fields (Firestore Timestamp type or datetime fields):

```json
{
  "name": "users",
  "replication_key": "updated_at",
  "replication_key_type": "timestamp"
}
```

### String-based Incremental

For string fields (e.g., ISO date strings, UUIDs):

```json
{
  "name": "events",
  "replication_key": "event_id",
  "replication_key_type": "string"
}
```

### Full Table Replication

For collections without incremental replication, omit the `replication_key`:

```json
{
  "name": "products"
}
```

## Data Type Handling

The tap automatically converts Firestore data types to JSON-serializable formats:

- Timestamps → ISO format strings
- References → Converted to string paths
- Bytes → UTF-8 decoded strings
- Maps → JSON objects
- Arrays → JSON arrays

## Schema

The tap generates a flexible schema for each collection:

- `_id`: Document ID (string)
- `_sdc_extracted_at`: Extraction timestamp (datetime)
- `<replication_key>`: The replication key field (if specified)
- Additional fields from the document are included dynamically

## Development

### Install Dependencies

```bash
poetry install
```

### Run Tests

```bash
poetry run pytest
```

### Format Code

```bash
poetry run black tap_firestore/
poetry run isort tap_firestore/
```

## License

Apache License 2.0

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## Support

For issues and questions, please open a GitHub issue.
