#!/bin/bash
set -e

echo "=========================================="
echo "Example 17: SQLite JSON to Parquet"
echo "=========================================="
echo ""

# Change to the example directory
cd "$(dirname "$0")"

# Clean up any existing database and output files
rm -f names.db
rm -f names_output.parquet

echo "Step 1: Creating SQLite database with JSON column..."
sqlite3 names.db < setup_db.sql
echo "✓ Database created"
echo ""

echo "Step 2: Verifying data in SQLite..."
echo "Showing table structure:"
sqlite3 names.db ".schema names"
echo ""
echo "Sample data:"
sqlite3 names.db "SELECT id, name, json_extract(doc, '$.city') as city, json_extract(doc, '$.age') as age FROM names LIMIT 3;"
echo ""

echo "Step 3: Running TinyETL with --preview to see the data..."
echo "Command: ../../target/release/tinyetl 'sqlite://names.db#names' 'names_output.parquet' --schema-file names_schema.yaml --preview 3"
echo ""
../../target/release/tinyetl \
  'sqlite://names.db#names' \
  'names_output.parquet' \
  --schema-file names_schema.yaml \
  --preview 3
echo ""

echo "Step 4: Running full ETL from SQLite (with JSON) to Parquet..."
../../target/release/tinyetl \
  'sqlite://names.db#names' \
  'names_output.parquet' \
  --schema-file names_schema.yaml
echo "✓ ETL completed"
echo ""

echo "Step 5: Verifying output Parquet file..."
if [ -f names_output.parquet ]; then
    echo "✓ Parquet file created: names_output.parquet"
    ls -lh names_output.parquet
    echo ""
    
    echo "Step 6: Reading back from Parquet with preview..."
    echo "Command: ../../target/release/tinyetl 'file:names_output.parquet' 'file:verify_output.json' --preview 2"
    echo ""
    ../../target/release/tinyetl \
      'names_output.parquet' \
      'verify_output.json' \
      --preview 2
    echo ""
    
    echo "Step 7: Converting Parquet back to JSON to verify JSON column..."
    ../../target/release/tinyetl \
      'names_output.parquet' \
      'verify_output.json'
    
    if [ -f verify_output.json ]; then
        echo "✓ JSON output created"
        echo ""
        echo "First record with JSON intact:"
        cat verify_output.json | head -1 | python3 -m json.tool 2>/dev/null || cat verify_output.json | head -1
        echo ""
    fi
else
    echo "✗ Parquet file was not created"
    exit 1
fi

echo "=========================================="
echo "✓ Example 17 completed successfully!"
echo "=========================================="
echo ""
echo "Summary:"
echo "  - Created SQLite table with JSON column"
echo "  - Populated with 5 records containing complex JSON documents"
echo "  - Used --preview to inspect data before transfer"
echo "  - Transferred from SQLite to Parquet preserving JSON"
echo "  - Verified JSON integrity by converting back to JSON format"
echo ""
echo "Key features demonstrated:"
echo "  • JSON as a tier-1 datatype"
echo "  • Schema validation with JSON type"
echo "  • Preview mode for data inspection"
echo "  • JSON preservation across different formats (SQLite → Parquet → JSON)"
