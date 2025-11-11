#!/bin/bash

# Configuration
NODE_LABEL="buildbuddy.io/rack=22.04,buildbuddy.io/pool=bb-executor-prod-vm30"
NODE_NAME_PATTERN="sjc-prod-r2204m05vm*"
KUBECTL_CONTEXT="bb-prod-us-sjc"
DRY_RUN=true  # Default to dry-run mode

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --no-dry-run|--execute)
            DRY_RUN=false
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--dry-run|--no-dry-run|--execute]"
            exit 1
            ;;
    esac
done

# Display mode
if [ "$DRY_RUN" = true ]; then
    echo "========================================"
    echo "DRY RUN MODE - No changes will be made"
    echo "========================================"
    echo ""
else
    echo "========================================"
    echo "EXECUTION MODE - Changes will be made!"
    echo "========================================"
    echo ""
    read -p "Are you sure you want to proceed with EXECUTION? (type 'yes' to continue): " confirm
    if [ "$confirm" != "yes" ]; then
        echo "Aborted."
        exit 0
    fi
    echo ""
fi

# Show what will happen
echo "This script will:"
echo "  Kubectl context: $KUBECTL_CONTEXT"
echo "  Node labels: $NODE_LABEL"
echo "  Node name pattern: $NODE_NAME_PATTERN"
echo "  Action: Check host_id and drain nodes"
echo ""

# Build the kubectl command
KUBECTL_CMD="kubectl --context=$KUBECTL_CONTEXT get nodes -l $NODE_LABEL -o jsonpath='{range .items[*]}{.metadata.name}{\" \"}{.status.addresses[?(@.type==\"ExternalIP\")].address}{\"\\n\"}{end}'"

# Get list of nodes and their external IPs
echo "Fetching nodes..."
if [ "$DRY_RUN" = true ]; then
    echo "[DRY RUN] Command: $KUBECTL_CMD"
fi

nodes_info=$(eval $KUBECTL_CMD)

if [ -z "$nodes_info" ]; then
    echo "No nodes found or error getting nodes"
    exit 1
fi

# Filter nodes by name pattern
if [ -n "$NODE_NAME_PATTERN" ]; then
    # Convert glob pattern to regex (simple conversion: * -> .*)
    pattern_regex=$(echo "$NODE_NAME_PATTERN" | sed 's/\*/.*/g')
    nodes_info=$(echo "$nodes_info" | grep -E "^${pattern_regex} ")

    if [ -z "$nodes_info" ]; then
        echo "No nodes found matching pattern: $NODE_NAME_PATTERN"
        exit 1
    fi
fi

echo "Found nodes:"
echo "$nodes_info"
echo ""

# Process each node
while IFS= read -r line; do
    node_name=$(echo $line | awk '{print $1}')
    external_ip=$(echo $line | awk '{print $2}')
    
    if [ -z "$node_name" ] || [ -z "$external_ip" ]; then
        echo "Skipping invalid entry: $line"
        continue
    fi
    
    echo ""
    echo "================================================"
    echo "Processing node: $node_name ($external_ip)"
    echo "================================================"

    # Step 1: Check host_id is non-empty
    echo "Step 1: Checking host_id on $external_ip..."
    CHECK_CMD="ssh core@$external_ip 'cat /var/buildbuddy/filecache/metadata/host_id'"
    echo "[DRY RUN] Would execute: $CHECK_CMD"

    if [ "$DRY_RUN" = false ]; then
        host_id=$(ssh core@$external_ip 'cat /var/buildbuddy/filecache/metadata/host_id' 2>/dev/null)
        if [ -z "$host_id" ]; then
            echo "✗ ERROR: host_id is empty on $external_ip"
            echo "Skipping this node for safety..."
            continue
        fi
        echo "✓ host_id is non-empty: $host_id"
    fi

    # Step 2: Drain the node
    echo "Step 2: Draining node $node_name..."
    DRAIN_CMD="kubectl --context=$KUBECTL_CONTEXT drain $node_name --ignore-daemonsets"
    echo "[DRY RUN] Would execute: $DRAIN_CMD"

    if [ "$DRY_RUN" = false ]; then
        if kubectl --context=$KUBECTL_CONTEXT drain $node_name --ignore-daemonsets; then
            echo "✓ Node drained successfully"
        else
            echo "✗ Failed to drain node $node_name"
            echo "Skipping this node..."
            continue
        fi
    fi

    echo "Completed processing $node_name"
    
done <<< "$nodes_info"

echo ""
echo "================================================"
if [ "$DRY_RUN" = true ]; then
    echo "DRY RUN completed - no changes were made"
    echo "Run with --no-dry-run or --execute to perform actual operations"
else
    echo "All nodes processed"
fi
echo "================================================"
