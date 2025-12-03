#!/bin/bash

# Test runner for TinyETL HTTP Protocol
# This script starts the HTTP server and runs various authentication tests

set -e

echo "======================================================================"
echo "TinyETL HTTP Protocol Test Suite"
echo "======================================================================"
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Change to test_env directory
cd "$(dirname "$0")"

# Function to check if server is ready
wait_for_server() {
    echo "Waiting for HTTP server to be ready..."
    for i in {1..30}; do
        if curl -s http://localhost:8080/health > /dev/null 2>&1; then
            echo -e "${GREEN}✓ Server is ready${NC}"
            return 0
        fi
        echo -n "."
        sleep 1
    done
    echo -e "${RED}✗ Server failed to start${NC}"
    return 1
}

# Start the HTTP server
echo "Step 1: Starting HTTP test server..."
echo "----------------------------------------------------------------------"
docker-compose up -d http-server

# Wait for server to be ready
if ! wait_for_server; then
    echo "Checking server logs:"
    docker-compose logs http-server
    exit 1
fi

echo ""
echo "Step 2: Running HTTP Protocol Tests"
echo "----------------------------------------------------------------------"
echo ""

# Test counter
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

# Function to run a test
run_test() {
    local test_name="$1"
    local config_file="$2"
    local expected_output="$3"
    
    TESTS_RUN=$((TESTS_RUN + 1))
    echo -n "Test $TESTS_RUN: $test_name... "
    
    # Clean up previous output
    rm -f "$expected_output"
    
    # Run TinyETL
    if ../../target/release/tinyetl run "$config_file" > /dev/null 2>&1 || \
       ../../target/debug/tinyetl run "$config_file" > /dev/null 2>&1 || \
       (cd ../.. && cargo run --quiet -- run "test_env/http-server/$config_file") > /dev/null 2>&1; then
        
        # Check if output file was created
        if [ -f "$expected_output" ]; then
            TESTS_PASSED=$((TESTS_PASSED + 1))
            echo -e "${GREEN}✓ PASSED${NC}"
            return 0
        else
            TESTS_FAILED=$((TESTS_FAILED + 1))
            echo -e "${RED}✗ FAILED (no output file)${NC}"
            return 1
        fi
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
        echo -e "${RED}✗ FAILED (execution error)${NC}"
        return 1
    fi
}

# Run tests
echo "Running authentication tests..."
echo ""

run_test "Public endpoint (no auth)" \
    "configs/public.yaml" \
    "output_public.json"

run_test "Basic authentication" \
    "configs/basic-auth.yaml" \
    "output_basic_auth.json"

run_test "Bearer token authentication" \
    "configs/bearer-auth.yaml" \
    "output_bearer_auth.json"

run_test "Custom headers" \
    "configs/custom-headers.yaml" \
    "output_custom_headers.json"

echo ""
echo "======================================================================"
echo "Test Results"
echo "======================================================================"
echo "Tests Run:    $TESTS_RUN"
echo -e "Tests Passed: ${GREEN}$TESTS_PASSED${NC}"
if [ $TESTS_FAILED -gt 0 ]; then
    echo -e "Tests Failed: ${RED}$TESTS_FAILED${NC}"
else
    echo -e "Tests Failed: $TESTS_FAILED"
fi
echo "======================================================================"

# Verify outputs
if [ $TESTS_PASSED -gt 0 ]; then
    echo ""
    echo "Sample output from successful tests:"
    echo "----------------------------------------------------------------------"
    
    if [ -f "output_basic_auth.json" ]; then
        echo "Basic Auth output (first 3 lines):"
        head -n 3 output_basic_auth.json
        echo ""
    fi
    
    if [ -f "output_bearer_auth.json" ]; then
        echo "Bearer Auth output (first 3 lines):"
        head -n 3 output_bearer_auth.json
        echo ""
    fi
fi

# Clean up outputs
echo ""
echo "Cleaning up test outputs..."
rm -f output_*.json

echo ""
if [ $TESTS_FAILED -eq 0 ]; then
    echo -e "${GREEN}✓ All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}✗ Some tests failed${NC}"
    echo ""
    echo "To debug, check server logs:"
    echo "  docker-compose logs http-server"
    echo ""
    echo "To test manually:"
    echo "  curl http://localhost:8080/health"
    echo "  curl -u testuser:testpass http://localhost:8080/basic-auth/users.csv"
    exit 1
fi
