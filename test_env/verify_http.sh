#!/bin/bash

# Quick verification script for HTTP protocol implementation
# This script verifies that the HTTP server and TinyETL integration work correctly

set -e

echo "========================================================================"
echo "TinyETL HTTP Protocol - Quick Verification"
echo "========================================================================"
echo ""

cd "$(dirname "$0")"

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "❌ docker-compose not found. Please install Docker."
    exit 1
fi

echo "✓ Docker Compose found"

# Start the HTTP server
echo ""
echo "Starting HTTP test server..."
docker-compose up -d http-server 2>&1 | grep -v "WARNING"

# Wait for it to be ready
echo "Waiting for server to be ready..."
for i in {1..15}; do
    if curl -s http://localhost:8080/health > /dev/null 2>&1; then
        echo "✓ HTTP server is running on port 8080"
        break
    fi
    if [ $i -eq 15 ]; then
        echo "❌ Server failed to start. Check logs with: docker-compose logs http-server"
        exit 1
    fi
    sleep 1
done

# Test endpoints
echo ""
echo "Testing endpoints..."
echo "--------------------------------------------------------------------"

# Test 1: Public endpoint
echo -n "1. Public endpoint (no auth)... "
if curl -s http://localhost:8080/public/data.csv | grep -q "Alice Johnson"; then
    echo "✓ PASS"
else
    echo "❌ FAIL"
fi

# Test 2: Basic auth endpoint
echo -n "2. Basic authentication... "
if curl -s -u testuser:testpass http://localhost:8080/basic-auth/users.csv | grep -q "Alice Johnson"; then
    echo "✓ PASS"
else
    echo "❌ FAIL"
fi

# Test 3: Basic auth with wrong password
echo -n "3. Basic auth rejection (wrong password)... "
if curl -s -u testuser:wrongpass http://localhost:8080/basic-auth/users.csv | grep -q "401"; then
    echo "✓ PASS"
else
    echo "❌ FAIL"
fi

# Test 4: Bearer token endpoint
echo -n "4. Bearer token authentication... "
if curl -s -H "Authorization: Bearer test-bearer-token-12345" http://localhost:8080/bearer-auth/products.csv | grep -q "Laptop"; then
    echo "✓ PASS"
else
    echo "❌ FAIL"
fi

# Test 5: Bearer token rejection
echo -n "5. Bearer token rejection (wrong token)... "
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer wrong-token" http://localhost:8080/bearer-auth/products.csv)
if [ "$HTTP_CODE" = "403" ]; then
    echo "✓ PASS"
else
    echo "❌ FAIL (got HTTP $HTTP_CODE)"
fi

# Test 6: Custom headers
echo -n "6. Custom headers validation... "
if curl -s -H "X-API-Version: v2" http://localhost:8080/custom-headers/data.csv | grep -q "Alice Johnson"; then
    echo "✓ PASS"
else
    echo "❌ FAIL"
fi

# Test 7: Custom headers rejection
echo -n "7. Custom headers rejection (missing header)... "
if curl -s http://localhost:8080/custom-headers/data.csv | grep -q "400"; then
    echo "✓ PASS"
else
    echo "❌ FAIL"
fi

echo ""
echo "========================================================================"
echo "Server Information"
echo "========================================================================"
curl -s http://localhost:8080/ | python3 -m json.tool 2>/dev/null || \
    curl -s http://localhost:8080/

echo ""
echo "========================================================================"
echo "Next Steps"
echo "========================================================================"
echo ""
echo "The HTTP server is running and ready for testing!"
echo ""
echo "To test with TinyETL:"
echo "  1. Build TinyETL: cargo build --release"
echo "  2. Run tests: cd http-server && ./test.sh"
echo ""
echo "To test endpoints manually:"
echo "  curl http://localhost:8080/public/data.csv"
echo "  curl -u testuser:testpass http://localhost:8080/basic-auth/users.csv"
echo "  curl -H 'Authorization: Bearer test-bearer-token-12345' \\"
echo "       http://localhost:8080/bearer-auth/products.csv"
echo ""
echo "To stop the server:"
echo "  docker-compose down"
echo ""
