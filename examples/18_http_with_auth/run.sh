#!/bin/bash

# Example: HTTP Download with Custom Headers and Authentication
# This example demonstrates TinyETL's ability to download files from HTTP sources
# with custom headers and authentication.

set -e  # Exit on error

echo "=== HTTP Source with Custom Headers and Authentication ==="
echo ""

# Clean up any previous output
rm -f output.csv test_output.json

echo "Step 1: Testing HTTP download with custom headers (no auth required)"
echo "-----------------------------------------------------------------------"
echo "Downloading sample CSV from a public source..."
echo ""

# Create a simple config for testing with a public CSV URL
cat > test_config.yaml <<EOF
version: 1

source:
  uri: "https://raw.githubusercontent.com/datasets/gdp/master/data/gdp.csv"
  options:
    header.User-Agent: "TinyETL/0.9.0"
    header.Accept: "text/csv"

target:
  uri: "test_output.json"
  options: {}

options:
  batch_size: 1000
  log_level: info
  preview: 5
EOF

echo "Running TinyETL with custom headers..."
../../target/release/tinyetl run test_config.yaml 2>&1 || {
    echo "Note: If you haven't built the release version, run: cargo build --release"
    echo "Trying debug build instead..."
    ../../target/debug/tinyetl run test_config.yaml 2>&1 || {
        echo "Building and running..."
        (cd ../.. && cargo run -- run examples/18_http_with_auth/test_config.yaml)
    }
}

if [ -f test_output.json ]; then
    echo ""
    echo "✓ Successfully downloaded and processed data with custom headers!"
    echo ""
    echo "Preview of downloaded data:"
    head -n 10 test_output.json
else
    echo "✗ Output file not created"
fi

echo ""
echo "-----------------------------------------------------------------------"
echo "Step 2: Configuration with Authentication (for your own APIs)"
echo "-----------------------------------------------------------------------"
echo ""
echo "For APIs requiring authentication, use this configuration pattern:"
cat config.yaml

echo ""
echo "Protocol Options Available:"
echo "  • header.<name>         : Add custom HTTP headers"
echo "  • auth.basic.username   : Basic authentication username"
echo "  • auth.basic.password   : Basic authentication password"
echo "  • auth.bearer           : Bearer token authentication"
echo ""
echo "Example with environment variables:"
echo "  export API_USERNAME=your_username"
echo "  export API_PASSWORD=your_password"
echo "  tinyetl run config.yaml"
echo ""
echo "Example with bearer token:"
echo "  export API_TOKEN=your_token_here"
echo "  # Update config.yaml to use: auth.bearer: \"\${API_TOKEN}\""
echo "  tinyetl run config.yaml"
echo ""

# Clean up test file
rm -f test_config.yaml

echo "✓ Example completed successfully!"
echo ""
