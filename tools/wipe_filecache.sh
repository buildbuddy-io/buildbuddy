#!/bin/bash

# Configuration
NODE_LABEL=""     # Optional: Add label selector like "role=worker"
DRY_RUN=true      # Default to dry-run mode

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
        --node-label)
            NODE_LABEL="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--dry-run|--no-dry-run|--execute] [--node-label <label>]"
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
if [ -n "$NODE_LABEL" ]; then
    echo "  Node label filter: $NODE_LABEL"
else
    echo "  Node label filter: (none - all nodes)"
fi
echo "  Command: sudo rm -rf \"/var/buildbuddy/filecache/\$(cat /var/buildbuddy/filecache/metadata/host_id)/*\""
echo ""

# Build the kubectl command
KUBECTL_CMD="kubectl get nodes"
if [ -n "$NODE_LABEL" ]; then
    KUBECTL_CMD="$KUBECTL_CMD -l $NODE_LABEL"
fi
KUBECTL_CMD="$KUBECTL_CMD -o jsonpath='{range .items[*]}{.metadata.name}{\" \"}{.status.addresses[?(@.type==\"ExternalIP\")].address}{\"\\n\"}{end}'"

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
    
    # Step 1: Drain the node
    echo "Step 1: Draining node $node_name..."
    DRAIN_CMD="kubectl drain $node_name --ignore-daemonsets --delete-emptydir-data --force --grace-period=30"
    echo "[DRY RUN] Would execute: $DRAIN_CMD"
    
#    if [ "$DRY_RUN" = false ]; then
#        if $DRAIN_CMD; then
#            echo "✓ Node drained successfully"
#        else
#            echo "✗ Failed to drain node $node_name"
#            echo "Skipping this node..."
#            continue
#        fi
#    fi
    
    # Step 2: SSH and clean
    echo "Step 2: SSHing into $external_ip and cleaning BuildBuddy filecache..."
    SSH_CMD="ssh $external_ip 'sudo rm -rf \"/var/buildbuddy/filecache/\$(cat /var/buildbuddy/filecache/metadata/host_id)/*\"'"
    echo "[DRY RUN] Would execute: $SSH_CMD"

#    if [ "$DRY_RUN" = false ]; then
#        if ssh $external_ip 'sudo rm -rf "/var/buildbuddy/filecache/$(cat /var/buildbuddy/filecache/metadata/host_id)/*"'; then
#            echo "✓ Cleaned BuildBuddy filecache successfully"
#        else
#            echo "✗ Failed to clean filecache on $external_ip"
#            echo "Attempting to uncordon anyway..."
#        fi
#    fi
    
    # Step 3: Uncordon the node
    echo "Step 3: Uncordoning node $node_name..."
    UNCORDON_CMD="kubectl uncordon $node_name"
    echo "[DRY RUN] Would execute: $UNCORDON_CMD"
    
#    if [ "$DRY_RUN" = false ]; then
#        if $UNCORDON_CMD; then
#            echo "✓ Node uncordoned successfully"
#        else
#            echo "✗ Failed to uncordon node $node_name"
#        fi
#    fi
    
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
