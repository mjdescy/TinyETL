#!/bin/bash

# TinyETL YAML Configuration Examples
# This script demonstrates the new config file functionality

set -e

echo "=== TinyETL YAML Configuration Examples ==="
echo ""

# Build the binary if it doesn't exist
if [ ! -f "../../target/release/tinyetl" ]; then
    echo "Building TinyETL..."
    cd ../..
    cargo build --release
    cd examples/13_yaml_config
fi

TINYETL="../../target/release/tinyetl"

echo "Example 1: Basic Config File (CSV to JSON with transforms)"
echo "Command: tinyetl run basic_config.yaml"
echo ""
$TINYETL run basic_config.yaml

echo ""
echo "Output file created: employees_output.json"
echo "Preview of output:"
head -n 5 employees_output.json
echo ""

echo "=== Preview Mode Example ==="
echo "Command: tinyetl run basic_config.yaml (shows preview from config)"
echo ""

echo "=== Environment Variables Example ==="
echo "Setting up environment variables for advanced example..."

# Set environment variables for the advanced example
export DB_USER="demo_user"
export DB_PASSWORD="demo_pass" 
export DB_HOST="localhost"
export DB_PORT="5432"
export DB_NAME="demo_db"
export OUTPUT_DIR="/tmp/tinyetl"
export SCHEMA_DIR="../../schemas"
export TRANSFORM_DIR="../../transforms"

# Create output directory
mkdir -p "$OUTPUT_DIR"

echo "Environment variables set:"
echo "  DB_USER=$DB_USER"
echo "  DB_HOST=$DB_HOST"
echo "  OUTPUT_DIR=$OUTPUT_DIR"
echo ""

echo "Note: To test the advanced_config.yaml and s3_config.yaml examples,"
echo "you would need to set up actual database credentials and AWS credentials."
echo ""

echo "Example commands for environment variable configs:"
echo "  tinyetl run advanced_config.yaml"
echo "  tinyetl run s3_config.yaml"
echo ""

echo "=== Comparison: CLI vs Config File ==="
echo ""
echo "Traditional CLI command:"
echo "  tinyetl employees.csv employees_cli.json --batch-size 1000 --preview 5 \\"
echo "    --transform 'full_name = row.first_name .. \" \" .. row.last_name'"
echo ""

echo "Equivalent config file approach:"
echo "  tinyetl run basic_config.yaml"
echo ""

echo "Benefits of config files:"
echo "  ✓ Reusable and version-controllable"
echo "  ✓ Support for complex multi-line transforms"
echo "  ✓ Environment variable substitution with \${VAR} syntax"
echo "  ✓ Better organization for complex ETL jobs"
echo "  ✓ Easier to share and document"
echo ""

echo "Config file features demonstrated:"
echo "  ✓ Basic source/target configuration"
echo "  ✓ Inline Lua transforms with multi-line support"
echo "  ✓ Environment variable substitution"
echo "  ✓ All CLI options available in YAML format"
echo ""

echo "Done! Check the output files to see the results."
