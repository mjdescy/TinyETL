#!/bin/bash
# Example 10: CSV to Avro conversion

set -e
cd "$(dirname "$0")"

echo "Running Example 10: CSV to Avro"
echo "Input: input.csv -> Output: output.avro"

# Clean up any existing output
rm -f output.avro

# Run tinyetl command
../../target/release/tinyetl input.csv output.avro

# Validate output exists and has content
if [ ! -f "output.avro" ]; then
    echo "❌ FAIL: output.avro was not created"
    exit 1
fi

# Check if output file has reasonable size (should not be empty)
file_size=$(wc -c < output.avro)
if [ "$file_size" -eq 0 ]; then
    echo "❌ FAIL: output.avro is empty"
    exit 1
fi

echo "✅ SUCCESS: CSV to Avro conversion completed"
echo "   - Output file: output.avro"
echo "   - File size: $file_size bytes"

# Optional: If we had avro-tools installed, we could validate the schema
# echo "Avro file schema:"
# avro-tools getschema output.avro 2>/dev/null || echo "   (avro-tools not available for schema validation)"

echo "   - Note: Avro is a binary format. Use Avro tools to inspect contents."
