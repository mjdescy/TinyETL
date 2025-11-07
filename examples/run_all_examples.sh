#!/bin/bash
# Master test runner for all tinyetl examples
# This script builds the project and runs all examples with validation

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

print_header() {
    echo
    print_status "$BLUE" "================================================"
    print_status "$BLUE" "$1"
    print_status "$BLUE" "================================================"
}

cd "$(dirname "$0")"
PROJECT_ROOT="$(pwd)/.."

print_header "TinyETL Examples Test Suite"

# Step 1: Check if binary exists
print_header "Checking for tinyetl binary..."
TINYETL_BIN="../target/release/tinyetl"

if [ ! -f "$TINYETL_BIN" ]; then
    print_status "$RED" "❌ Binary not found at $TINYETL_BIN"
    print_status "$YELLOW" "Please build the project first with: cargo build --release"
    exit 1
else
    print_status "$GREEN" "✅ Found tinyetl binary"
fi

# Step 2: Run all examples
examples_dir="$(pwd)"
total_tests=0
passed_tests=0
failed_tests=0

# Find all example directories and run them
for example_dir in "$examples_dir"/*/; do
    if [ -d "$example_dir" ]; then
        example_name=$(basename "$example_dir")
        run_script="$example_dir/run.sh"
        
        if [ -f "$run_script" ]; then
            print_header "Running $example_name"
            total_tests=$((total_tests + 1))
            
            # Make script executable
            chmod +x "$run_script"
            
            # Run the example in its directory
            cd "$example_dir"
            
            # Clean up any previous output files
            rm -f output.* 2>/dev/null || true
            
            if bash run.sh; then
                print_status "$GREEN" "✅ $example_name PASSED"
                passed_tests=$((passed_tests + 1))
            else
                print_status "$RED" "❌ $example_name FAILED"
                failed_tests=$((failed_tests + 1))
            fi
            
            echo
        else
            print_status "$YELLOW" "⚠️  No run.sh script found in $example_name"
        fi
    fi
done

# Step 2: Summary
cd "$examples_dir"
print_header "Test Summary"

echo "Total tests: $total_tests"
print_status "$GREEN" "Passed: $passed_tests"
if [ $failed_tests -gt 0 ]; then
    print_status "$RED" "Failed: $failed_tests"
fi

if [ $failed_tests -eq 0 ]; then
    print_status "$GREEN" "🎉 All tests passed!"
    exit 0
else
    print_status "$RED" "💥 $failed_tests test(s) failed"
    exit 1
fi
