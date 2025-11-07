#!/bin/bash
# Example 2: File protocol with explicit file:// URLs

set -e
cd "$(dirname "$0")"

echo "Running Example 2: File protocol with explicit URLs"
echo "Input: file://$(pwd)/input.csv -> Output: file://$(pwd)/output.parquet"

# Convert to absolute paths for file:// URLs
input_path="file://$(pwd)/input.csv"
output_path="file://$(pwd)/output.parquet"

# Run tinyetl command with file protocol
../../target/release/tinyetl "$input_path" "$output_path"

# Validate output exists
if [ ! -f "output.parquet" ]; then
    echo "❌ FAIL: output.parquet was not created"
    exit 1
fi

# Check file size (parquet should have some content)
file_size=$(stat -c%s "output.parquet" 2>/dev/null || stat -f%z "output.parquet" 2>/dev/null || echo "0")
if [ "$file_size" -gt 0 ]; then
    echo "✅ PASS: Parquet file created with size $file_size bytes"
else
    echo "❌ FAIL: Parquet file is empty or not created"
    exit 1
fi

echo "✅ Example 2 completed successfully"
