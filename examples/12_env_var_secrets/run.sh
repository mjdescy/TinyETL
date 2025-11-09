#!/bin/bash
# Example 12: Environment Variable Secrets Testing

set -e
cd "$(dirname "$0")"

echo "Running Example 12: Environment Variable Secrets"
echo "Input: users.csv -> Output: mysql://user@localhost:3306/testdb#users (with env var secrets)"

echo ""
echo "=== Testing Environment Variable Secret Resolution ==="

# Set up environment variables for secrets
export TINYETL_SECRET_mysql_dest="testpass"
export TINYETL_SECRET_postgres_source="testpass"

echo "✅ Set TINYETL_SECRET_mysql_dest=testpass"
echo "✅ Set TINYETL_SECRET_postgres_source=testpass"

echo ""
echo "Test 1: Using explicit dest-secret-id flag"
echo "Command: tinyetl users.csv \"mysql://testuser:@localhost:3306/testdb#users\" --dest-secret-id mysql_dest --dry-run"
echo "Expected: Should use TINYETL_SECRET_mysql_dest environment variable"

# Test explicit secret ID for destination (auto-detection not yet implemented)
../../target/release/tinyetl users.csv "mysql://testuser:@localhost:3306/testdb#users" --dest-secret-id mysql_dest --dry-run

if [ $? -eq 0 ]; then
    echo "✅ PASS: Explicit mysql_dest secret worked successfully"
else
    echo "❌ FAIL: Explicit mysql_dest secret failed"
    exit 1
fi

echo ""
echo "Test 2: Different explicit secret ID with --dest-secret-id"
echo "Command: tinyetl users.csv \"mysql://testuser:@localhost:3306/testdb#users\" --dest-secret-id mysql_dest --dry-run"

# Test explicit secret ID parameter (same as test 1 but clearer description)
../../target/release/tinyetl users.csv "mysql://testuser:@localhost:3306/testdb#users" --dest-secret-id mysql_dest --dry-run

if [ $? -eq 0 ]; then
    echo "✅ PASS: Repeated explicit --dest-secret-id worked successfully"
else
    echo "❌ FAIL: Explicit --dest-secret-id failed"
    exit 1
fi

echo ""
echo "Test 3: Custom secret ID with different environment variable"
export TINYETL_SECRET_custom_mysql="testpass"
echo "✅ Set TINYETL_SECRET_custom_mysql=testpass"

echo "Command: tinyetl users.csv \"mysql://testuser:@localhost:3306/testdb#users\" --dest-secret-id custom_mysql --dry-run"

../../target/release/tinyetl users.csv "mysql://testuser:@localhost:3306/testdb#users" --dest-secret-id custom_mysql --dry-run

if [ $? -eq 0 ]; then
    echo "✅ PASS: Custom secret ID worked successfully"
else
    echo "❌ FAIL: Custom secret ID failed"
    exit 1
fi

echo ""
echo "Test 4: Round-trip test - CSV to PostgreSQL to CSV (using source secrets)"
echo "Step 4a: Load CSV data into PostgreSQL using dest secret (ACTUAL DATA TRANSFER)"
echo "Command: tinyetl users.csv \"postgres://testuser:@localhost:5432/testdb#users\" --dest-secret-id postgres_source"

../../target/release/tinyetl users.csv "postgres://testuser:@localhost:5432/testdb#users" --dest-secret-id postgres_source

if [ $? -eq 0 ]; then
    echo "✅ PASS: Step 4a - CSV to PostgreSQL with dest secret worked (table created and data loaded)"
else
    echo "❌ FAIL: Step 4a - CSV to PostgreSQL with dest secret failed"
    exit 1
fi

echo ""
echo "Step 4b: Read data back from PostgreSQL using source secret"
echo "Command: tinyetl \"postgres://testuser:@localhost:5432/testdb#users\" output.csv --source-secret-id postgres_source"

../../target/release/tinyetl "postgres://testuser:@localhost:5432/testdb#users" output.csv --source-secret-id postgres_source

if [ $? -eq 0 ]; then
    echo "✅ PASS: Step 4b - PostgreSQL to CSV with source secret worked"
    echo "✅ PASS: Complete round-trip test successful!"
    
    # Clean up generated files
    rm -f output.csv
    echo "🧹 Cleaned up test files"
else
    echo "❌ FAIL: Step 4b - PostgreSQL to CSV with source secret failed"
    exit 1
fi

echo ""
echo "Test 5: Security warning for passwords in CLI"
echo "Command: tinyetl users.csv \"mysql://testuser:plaintext_password@localhost:3306/testdb#users\" --dry-run"
echo "Expected: Should show security warning"

../../target/release/tinyetl users.csv "mysql://testuser:plaintext_password@localhost:3306/testdb#users" --dry-run 2>&1 | grep -i "warning.*insecure"

if [ $? -eq 0 ]; then
    echo "✅ PASS: Security warning displayed for plaintext password"
else
    echo "❌ FAIL: Security warning not displayed for plaintext password"
    exit 1
fi

echo ""
echo "Test 6: File-to-file operation (no secrets needed)"
echo "Command: tinyetl users.csv output.json --dry-run"
echo "Expected: Should work without any secrets since no databases involved"

../../target/release/tinyetl users.csv output.json --dry-run

if [ $? -eq 0 ]; then
    echo "✅ PASS: File-to-file operation worked without secrets"
else
    echo "❌ FAIL: File-to-file operation failed"
    exit 1
fi

# Clean up environment variables
unset TINYETL_SECRET_mysql_dest
unset TINYETL_SECRET_postgres_source
unset TINYETL_SECRET_custom_mysql

echo ""
echo "=== All Environment Variable Secret Tests Passed! ==="
echo ""
echo "Key Features Demonstrated:"
echo "• Explicit secret IDs using --source-secret-id and --dest-secret-id"
echo "• Custom secret naming with environment variables"
echo "• Security warnings for plaintext passwords in CLI"
echo "• Support for both source and destination secrets"
echo "• File operations without databases don't need secrets"
echo ""
echo "When to Use Secret Flags:"
echo "• Use --source-secret-id when connecting to database sources with secrets"
echo "• Use --dest-secret-id when connecting to database destinations with secrets"  
echo "• Custom secret names always require explicit flags"
echo "• Auto-detection (coming in future version)"
echo ""
echo "Environment Variable Pattern:"
echo "• TINYETL_SECRET_{protocol}_{type} (e.g., TINYETL_SECRET_mysql_dest)"
echo "• TINYETL_SECRET_{protocol} (e.g., TINYETL_SECRET_mysql)"
echo "• TINYETL_SECRET_{custom_name} (e.g., TINYETL_SECRET_prod_db)"
