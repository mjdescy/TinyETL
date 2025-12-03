#!/bin/bash

# Example 18: HTTP Download with Custom Headers and Authentication
# This example demonstrates TinyETL's HTTP protocol with various authentication methods
# using the local test HTTP server

set -e  # Exit on error

echo "======================================================================"
echo "Example 18: HTTP Source with Custom Headers and Authentication"
echo "======================================================================"
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Clean up any previous output
rm -f output_*.json output_*.csv

# Check if HTTP server is running
echo "Step 1: Checking HTTP Server Status"
echo "----------------------------------------------------------------------"
if curl -s http://localhost:8680/health > /dev/null 2>&1; then
    echo -e "${GREEN}✓ HTTP server is running${NC}"
else
    echo -e "${YELLOW}⚠ HTTP server is not running${NC}"
    echo ""
    echo "Starting the HTTP server..."
    echo "Please run from the test_env directory:"
    echo "  cd ../../test_env"
    echo "  docker-compose up -d http-server"
    echo ""
    echo "Then run this example again."
    exit 1
fi

echo ""
echo "Step 2: Testing Public Endpoint (No Authentication)"
echo "----------------------------------------------------------------------"
echo "Configuration: public_config.yaml"
echo ""

../../target/release/tinyetl run public_config.yaml 2>&1 || \
    ../../target/debug/tinyetl run public_config.yaml 2>&1 || \
    (cd ../.. && cargo run --quiet -- run examples/18_http_with_auth/public_config.yaml)

if [ -f output_public.json ]; then
    echo -e "${GREEN}✓ Success!${NC} Downloaded from public endpoint"
    echo "Preview:"
    head -n 3 output_public.json
    echo ""
else
    echo -e "${RED}✗ Failed${NC}"
fi

echo ""
echo "Step 3: Testing Basic Authentication"
echo "----------------------------------------------------------------------"
echo "Configuration: config.yaml"
echo "Credentials: testuser / testpass"
echo ""

../../target/release/tinyetl run config.yaml 2>&1 || \
    ../../target/debug/tinyetl run config.yaml 2>&1 || \
    (cd ../.. && cargo run --quiet -- run examples/18_http_with_auth/config.yaml)

if [ -f output_basic_auth.json ]; then
    echo -e "${GREEN}✓ Success!${NC} Downloaded with Basic Auth"
    echo "Preview:"
    head -n 3 output_basic_auth.json
    echo ""
else
    echo -e "${RED}✗ Failed${NC}"
fi

echo ""
echo "Step 4: Testing Bearer Token Authentication"
echo "----------------------------------------------------------------------"
echo "Configuration: bearer_config.yaml"
echo "Token: test-bearer-token-12345 (via environment variable)"
echo ""

# Set the bearer token as an environment variable
export BEARER_TOKEN="test-bearer-token-12345"

../../target/release/tinyetl run bearer_config.yaml 2>&1 || \
    ../../target/debug/tinyetl run bearer_config.yaml 2>&1 || \
    (cd ../.. && cargo run --quiet -- run examples/18_http_with_auth/bearer_config.yaml)

if [ -f output_bearer_auth.json ]; then
    echo -e "${GREEN}✓ Success!${NC} Downloaded with Bearer Token (from \$BEARER_TOKEN)"
    echo "Preview:"
    head -n 3 output_bearer_auth.json
    echo ""
else
    echo -e "${RED}✗ Failed${NC}"
fi

echo ""
echo "Step 5: Testing Custom Headers"
echo "----------------------------------------------------------------------"
echo "Configuration: custom_headers_config.yaml"
echo "Required Header: X-API-Version: v2"
echo ""

../../target/release/tinyetl run custom_headers_config.yaml 2>&1 || \
    ../../target/debug/tinyetl run custom_headers_config.yaml 2>&1 || \
    (cd ../.. && cargo run --quiet -- run examples/18_http_with_auth/custom_headers_config.yaml)

if [ -f output_custom_headers.json ]; then
    echo -e "${GREEN}✓ Success!${NC} Downloaded with custom headers"
    echo "Preview:"
    head -n 3 output_custom_headers.json
    echo ""
else
    echo -e "${RED}✗ Failed${NC}"
fi

echo ""
echo "======================================================================"
echo "Summary"
echo "======================================================================"
echo ""
echo "Protocol Options Demonstrated:"
echo "  ✓ header.<name>         - Custom HTTP headers"
echo "  ✓ auth.basic.username   - Basic authentication username"
echo "  ✓ auth.basic.password   - Basic authentication password"
echo "  ✓ auth.bearer           - Bearer token authentication"
echo ""
echo "Available Configurations:"
echo "  • public_config.yaml          - No authentication"
echo "  • config.yaml                 - Basic authentication"
echo "  • bearer_config.yaml          - Bearer token authentication"
echo "  • custom_headers_config.yaml  - Custom headers validation"
echo ""
echo "Test HTTP Server Endpoints:"
echo "  http://localhost:8680/public/data.csv"
echo "  http://localhost:8680/basic-auth/users.csv"
echo "  http://localhost:8680/bearer-auth/products.csv"
echo "  http://localhost:8680/custom-headers/data.csv"
echo ""
echo "For real-world usage with external APIs:"
echo "  1. Update the 'uri' in config files to your API endpoint"
echo "  2. Use environment variables for sensitive credentials:"
echo "     source:"
echo "       uri: \"https://api.example.com/data.csv\""
echo "       options:"
echo "         auth.basic.username: \"\${API_USERNAME}\""
echo "         auth.basic.password: \"\${API_PASSWORD}\""
echo ""
echo "Clean up outputs:"
echo "  rm output_*.json"
echo ""
echo -e "${GREEN}✓ Example completed successfully!${NC}"
echo ""
