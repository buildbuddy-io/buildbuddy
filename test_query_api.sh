#!/bin/bash

# Test script for the Bazel Query API
# Usage: ./test_query_api.sh

# Configuration - update these values
# API_KEY="${BUILDBUDDY_API_KEY:TeexEGgC9dv6007PeA2d}"
API_KEY="${BUILDBUDDY_API_KEY:-ni8F2v0unFMZYjeVN7mv}"
#API_ENDPOINT="${BUILDBUDDY_API_ENDPOINT:-https://app.buildbuddy.io}"
API_ENDPOINT="${BUILDBUDDY_API_ENDPOINT:-http://localhost:8080}"
REPO_URL="${REPO_URL:-https://github.com/buildbuddy-io/buildbuddy}"
BRANCH="${BRANCH:-master}"
QUERY="${QUERY:-//server/util/alert/...}"  # Example query

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Testing Bazel Query API${NC}"
echo "================================"
echo "Endpoint: $API_ENDPOINT"
echo "Repository: $REPO_URL"
echo "Branch: $BRANCH"
echo "Query: $QUERY"
echo "================================"

# Check if API key is set
if [ "$API_KEY" == "your-api-key-here" ]; then
    echo -e "${RED}Error: Please set BUILDBUDDY_API_KEY environment variable${NC}"
    echo "Example: export BUILDBUDDY_API_KEY=your-actual-api-key"
    exit 1
fi

# Create the JSON request body
REQUEST_BODY=$(cat <<EOF
{
  "repo": "$REPO_URL",
  "branch": "$BRANCH",
  "query": "$QUERY",
  "platform_properties": {
    "OSFamily": "darwin",
    "Arch": "arm64",
    "workload-isolation-type": "none"
  },
  "timeout": "30s",
  "bazel_flags": [
    "--bes_backend=grpc://localhost:1985"
  ]
}
EOF
)

echo -e "\n${YELLOW}Request body:${NC}"
echo "$REQUEST_BODY" | jq '.' 2>/dev/null || echo "$REQUEST_BODY"

# Make the API request
echo -e "\n${YELLOW}Making API request...${NC}"

RESPONSE=$(curl -s -X POST \
  "$API_ENDPOINT/api/v1/Query" \
  -H "Content-Type: application/json" \
  -H "x-buildbuddy-api-key: $API_KEY" \
  -d "$REQUEST_BODY")

# Check if curl succeeded
if [ $? -ne 0 ]; then
    echo -e "${RED}Error: Failed to make API request${NC}"
    exit 1
fi

# Pretty print the response
echo -e "\n${GREEN}Response:${NC}"
echo "$RESPONSE" | jq '.' 2>/dev/null || echo "$RESPONSE"

# Extract invocation ID if present
INVOCATION_ID=$(echo "$RESPONSE" | jq -r '.invocationId' 2>/dev/null)
if [ "$INVOCATION_ID" != "null" ] && [ -n "$INVOCATION_ID" ]; then
    echo -e "\n${GREEN}Success!${NC}"
    echo "Invocation ID: $INVOCATION_ID"
    echo "View invocation at: $API_ENDPOINT/invocation/$INVOCATION_ID"
    
    # Check if we got query results
    HAS_RESULTS=$(echo "$RESPONSE" | jq '.result.target | length' 2>/dev/null)
    if [ "$HAS_RESULTS" -gt 0 ] 2>/dev/null; then
        echo -e "\n${GREEN}Query returned $HAS_RESULTS targets${NC}"
        
        # Show first few targets as example
        echo -e "\n${YELLOW}First 5 targets:${NC}"
        echo "$RESPONSE" | jq -r '.result.target[:5][].rule.name' 2>/dev/null
    else
        echo -e "\n${YELLOW}Note: Query completed but no targets in result (may still be processing)${NC}"
    fi
else
    echo -e "\n${RED}Error: No invocation ID in response${NC}"
    echo "Response may contain an error message"
    exit 1
fi
