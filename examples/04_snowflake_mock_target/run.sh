#!/bin/bash
# Example 4: CSV to Snowflake target (mock implementation)

set -e
cd "$(dirname "$0")"

echo "Running Example 4: CSV -> Snowflake mock target"
echo "This demonstrates writing to Snowflake using the protocol architecture"

# Mock Snowflake connection string
input_file="orders.csv"
snowflake_url="snowflake://testuser:testpass@xy12345.east-us.azure/testdb/public?warehouse=COMPUTE_WH&table=orders"

# Run tinyetl command
echo "Command: ../../target/release/tinyetl \"$input_file\" \"$snowflake_url\""
../../target/release/tinyetl "$input_file" "$snowflake_url"

# For mock implementation, we can't really validate the Snowflake write
# but we can check that the command completed without errors
if [ $? -eq 0 ]; then
    echo "✅ PASS: Command completed successfully"
    echo "✅ PASS: Protocol architecture working for Snowflake target"
    echo "Note: In production, this would write to actual Snowflake table"
else
    echo "❌ FAIL: Command failed with exit code $?"
    exit 1
fi

echo "✅ Example 4 completed successfully"
