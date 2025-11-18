# Example 17: SQLite JSON to Parquet

This example demonstrates TinyETL's support for JSON as a tier-1 datatype, showing how to:
- Create a SQLite table with a JSON column
- Use a schema file to define JSON column types
- Preview data before running ETL
- Transfer JSON data from SQLite to Parquet
- Verify JSON integrity through round-trip conversion

## Features Demonstrated

- **JSON as Tier-1 Type**: Native support for JSON columns in schema definitions
- **Preview Mode**: Use `--preview` to inspect data before full transfer
- **JSON Preservation**: JSON structure maintained across format conversions
- **Schema Validation**: Explicit JSON type declaration in schema files

## Files

- `setup_db.sql` - Creates SQLite table with JSON column and sample data
- `names_schema.yaml` - Schema file defining column types including JSON
- `run.sh` - Complete example script with preview and verification

## Running the Example

```bash
cd examples/17_sqlite_json_to_parquet
./run.sh
```

## Sample Data

The example creates a `names` table with these columns:
- `id` (TEXT) - User identifier
- `url` (TEXT) - User profile URL  
- `name` (TEXT) - User name
- `doc` (JSON) - User metadata as JSON

Sample JSON documents include:
```json
{
  "age": 30,
  "city": "New York",
  "active": true,
  "tags": ["developer", "manager"]
}
```

## Schema Definition

```yaml
columns:
  - name: id
    type: string
    nullable: false
  - name: url
    type: string
    nullable: false
  - name: name
    type: string
    nullable: false
  - name: doc
    type: json  # JSON as tier-1 datatype
    nullable: false
```

## Expected Output

The script will:
1. Create SQLite database with JSON column
2. Show preview of 3 records before transfer
3. Transfer all data from SQLite to Parquet
4. Verify by converting Parquet back to JSON
5. Display sample record showing JSON integrity

## Preview Mode

The `--preview N` flag shows the first N rows without performing the transfer:

```bash
tinyetl 'sqlite:names.db?table=names' 'file:output.parquet' --preview 3
```

This is useful for:
- Verifying data structure before ETL
- Checking JSON formatting
- Validating schema mapping
- Quick data exploration

## JSON Support Across Connectors

TinyETL's JSON support varies by connector:
- **PostgreSQL**: Native `JSONB` type
- **MySQL**: Native `JSON` type
- **SQLite**: Stored as `TEXT`
- **DuckDB**: Native `JSON` type
- **Parquet**: UTF8 string via Arrow
- **CSV**: Compact JSON string
- **JSON files**: Native JSON objects
