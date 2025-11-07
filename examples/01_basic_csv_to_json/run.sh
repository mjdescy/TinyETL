#!/bin/bash
# Example 1: Basic CSV to JSON conversion (backward compatibility)

set -e
cd "$(dirname "$0")"

echo "Running Example 1: Basic CSV to JSON"
echo "Input: input.csv -> Output: output.json"

# Run tinyetl command
../../target/release/tinyetl input.csv output.json

# Validate output exists and has content
if [ ! -f "output.json" ]; then
    echo "❌ FAIL: output.json was not created"
    exit 1
fi

# Check if output has expected number of records
record_count=$(jq length output.json)
expected_count=5

if [ "$record_count" -eq "$expected_count" ]; then
    echo "✅ PASS: Found $record_count records (expected $expected_count)"
else
    echo "❌ FAIL: Found $record_count records, expected $expected_count"
    exit 1
fi

# Check if specific fields exist
if jq -e '.[0] | has("id") and has("name") and has("email")' output.json > /dev/null; then
    echo "✅ PASS: Output contains expected fields"
else
    echo "❌ FAIL: Output missing expected fields"
    exit 1
fi

echo "✅ Example 1 completed successfully"
